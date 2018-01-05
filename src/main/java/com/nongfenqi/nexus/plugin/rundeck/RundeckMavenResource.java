/*
 * Copyright 2017 黑牛
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the
 * Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
 * OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nongfenqi.nexus.plugin.rundeck;

import static com.google.common.base.Preconditions.*;
import static java.util.Collections.*;
import static javax.ws.rs.core.MediaType.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.sonatype.nexus.common.text.Strings2.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.apache.http.client.utils.DateUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.sonatype.goodies.common.ComponentSupport;
import org.sonatype.nexus.blobstore.api.Blob;
import org.sonatype.nexus.repository.Repository;
import org.sonatype.nexus.repository.manager.RepositoryManager;
import org.sonatype.nexus.repository.search.SearchService;
import org.sonatype.nexus.repository.storage.Asset;
import org.sonatype.nexus.repository.storage.Bucket;
import org.sonatype.nexus.repository.storage.StorageFacet;
import org.sonatype.nexus.repository.storage.StorageTx;
import org.sonatype.nexus.rest.Resource;

import com.google.common.base.Supplier;

@Named
@Singleton
@Path("/rundeck/maven/options")
public class RundeckMavenResource extends ComponentSupport implements Resource {
	private final SearchService searchService;

	private final RepositoryManager repositoryManager;

	private static final Response NOT_FOUND = Response.status(404).build();

	@Inject
	public RundeckMavenResource(SearchService searchService, RepositoryManager repositoryManager) {
		this.searchService = checkNotNull(searchService);
		this.repositoryManager = checkNotNull(repositoryManager);

		log.info("RundeckMavenResource: Constructor");
	}

	@GET
	@Path("content")
	public Response content(@QueryParam("r") String repositoryName, @QueryParam("g") String groupId,
			@QueryParam("a") String artifactId, @QueryParam("v") String version,
			@QueryParam("c") @DefaultValue("") String classifier,
			@QueryParam("e") @DefaultValue("jar") String extension) {

		// default version
		version = Optional.ofNullable(version)
				.orElse(latestVersion(repositoryName, groupId, artifactId, classifier, extension));

		// valid params
		if (isBlank(repositoryName) || isBlank(groupId) || isBlank(artifactId) || isBlank(version)) {
			log.debug("repositoryName: {}, groupId: {}, artifactId: {}, version: {}", repositoryName, groupId,
					artifactId, version);
			return NOT_FOUND;
		}

		Repository repository = repositoryManager.get(repositoryName);
		if (null == repository || !repository.getFormat().getValue().equals("maven2")) {
			return NOT_FOUND;
		}

		log.debug("Getting facet supplier");
		StorageFacet facet = repository.facet(StorageFacet.class);
		Supplier<StorageTx> storageTxSupplier = facet.txSupplier();

		log.debug("rundeck download repository: {}", repository);
		final StorageTx tx = storageTxSupplier.get();
		tx.begin();
		Bucket bucket = tx.findBucket(repository);
		log.debug("rundeck download bucket: {}", bucket);

		if (null == bucket) {
			return commitAndReturn(NOT_FOUND, tx);
		}

		String fileName = artifactId + "-" + version //
				+ (isBlank(classifier) ? "" : ("-" + classifier)) + "." //
				+ extension;

		String pathVersion = version;

		// Snapshots have special paths - strip off the timestamp and replace with SNAPSHOT
		if (repository.getName().equals("snapshots")) {
			// version MAY look like: 17.25-20180102.192026-6
			// OR it may just be 17.25-SNAPSHOT
			if (version.indexOf("-SNAPSHOT") < 0) {
				// this version is an actual file name so strip off the timestamp and replace with -SNAPSHOT
				log.debug("fileName: {}. version: {}, repository: {}", fileName, version, repository);
				pathVersion = version.substring(0, version.indexOf("-")) + "-SNAPSHOT";
			}
			else {
				// we need to find the latest snapshot and use that
				// no need to update pathVersion -it's already correct
				// filename needs to be resolved, though
				String fileVersion = latestSnapshot(repositoryName, groupId, artifactId, version);
				fileName = artifactId + "-" + fileVersion //
						+ (isBlank(classifier) ? "" : ("-" + classifier)) + "." //
						+ extension;
			}
		}

		String path = groupId.replace(".", "/") + "/" + artifactId + "/" + pathVersion + "/" + fileName;
		log.debug("path: {}", path);
		Asset asset = tx.findAssetWithProperty("name", path, bucket);
		log.debug("rundeck download asset: {}", asset);
		if (null == asset) {
			return commitAndReturn(NOT_FOUND, tx);
		}
		asset.markAsDownloaded();
		tx.saveAsset(asset);
		Blob blob = tx.requireBlob(asset.requireBlobRef());
		Response.ResponseBuilder ok = Response.ok(blob.getInputStream());
		ok.header("Content-Type", blob.getHeaders().get("BlobStore.content-type"));
		ok.header("Content-Disposition", "attachment;filename=\"" + fileName + "\"");
		return commitAndReturn(ok.build(), tx);
	}

	@GET
	@Path("version")
	@Produces(APPLICATION_JSON)
	public List<RundeckXO> version(@DefaultValue("10") @QueryParam("l") int limit,
			@QueryParam("r") @DefaultValue("") String repository, @QueryParam("g") String groupId,
			@QueryParam("a") String artifactId, @QueryParam("c") String classifier, @QueryParam("e") String extension) {

		log.info("param value, repository: {}, limit: {}, groupId: {}, artifactId: {}, classifier: {}, extension: {}",
				repository, limit, groupId, artifactId, classifier, extension);

		BoolQueryBuilder query = boolQuery();
		query.filter(termQuery("format", "maven2"));

		if (!isBlank(repository)) {
			query.filter(termQuery("repository_name", repository));
		}
		if (!isBlank(groupId)) {
			query.filter(termQuery("attributes.maven2.groupId", groupId));
		}
		if (!isBlank(artifactId)) {
			query.filter(termQuery("attributes.maven2.artifactId", artifactId));
		}
		if (!isBlank(classifier)) {
			query.filter(termQuery("assets.attributes.maven2.classifier", classifier));
		}
		if (!isBlank(extension)) {
			query.filter(termQuery("assets.attributes.maven2.extension", extension));
		}

		log.debug("rundeck maven version query: {}", query);

		// use a super large limit since we can't rely on the sort to sort in version order
		int bigLimit = Math.max(limit, 2000);
		SearchResponse result = searchService.search(query,
				Collections.singletonList(new FieldSortBuilder("assets.last_updated").order(SortOrder.DESC)), 0,
				bigLimit);
		log.debug("Result: {}", result);
		List<RundeckXO> results = Arrays.stream(result.getHits().hits()).map(this::his2RundeckXO)
				.collect(Collectors.toList());

		// Another sort by version number
		Collections.sort(results, new VersionComparator());

		// TODO: Filter out duplicates in snapshot versions
		// if (repository.equals("snapshots")) results = removeOldSnapshots(results);
		if (limit < results.size()) {
			results = results.subList(0, limit);
		}

		return results;
	}

	/**
	 * Gets the fileName for a specific snapshot.
	 *
	 * @param repository
	 * @param groupId
	 * @param artifactId
	 * @param version
	 * @return
	 */
	public String latestSnapshot(String repository, String groupId, String artifactId, String version) {

		log.info("param value, repository: {}, groupId: {}, artifactId: {}, version: {}", repository, groupId,
				artifactId, version);

		BoolQueryBuilder query = boolQuery();
		query.filter(termQuery("format", "maven2"));

		if (!isBlank(repository)) {
			query.filter(termQuery("repository_name", repository));
		}
		if (!isBlank(groupId)) {
			query.filter(termQuery("attributes.maven2.groupId", groupId));
		}
		if (!isBlank(artifactId)) {
			query.filter(termQuery("attributes.maven2.artifactId", artifactId));
		}
		if (!isBlank(version)) {
			query.filter(termQuery("assets.attributes.maven2.baseVersion", version));
		}

		log.debug("rundeck maven version query: {}", query);
		SearchResponse result = searchService.search(query, emptyList(), 0, 2000);
		log.debug("Result: {}", result);
		List<RundeckXO> results = Arrays.stream(result.getHits().hits()).map(this::his2RundeckXO)
				.collect(Collectors.toList());

		// Another sort by version number
		Collections.sort(results, new VersionComparator());
		return results.get(0).getValue();
	}

	/**
	 * Breaks a version string into a collection of parts. Each part is an Integer or a String. Anything not an int or
	 * character is treated as a delimiter. Parts are compared with each other until we get a non-zero result.
	 *
	 * Sample Versions: <br>
	 * <li>17.22-RC-20171110.165811-1
	 * <li>17.22-20171108.131206-6
	 * <li>17.20-20171017.181503-6
	 * <li>0.0.0-24-develop
	 *
	 * @author jwilliams
	 *
	 */
	class VersionComparator implements Comparator<RundeckXO> {
		class Version implements Comparable<Version> {
			List<Object> parts;

			public Version(String value) {
				parse(value);
			}

			private void parse(String value) {
				parts = new ArrayList<>();

				StringBuilder buffer = new StringBuilder();
				for (int i = 0; i < value.length(); i++) {
					int ch = value.charAt(i);

					if (Character.isLetterOrDigit(ch)) {
						buffer.append((char) ch);
					}
					else {
						if (buffer.length() > 0) {
							try {
								Integer val = Integer.parseInt(buffer.toString());
								parts.add(val);
							}
							catch (Exception e) {
								// not an int - just add the string
								parts.add(buffer.toString());
							}
							buffer.setLength(0);
						}
					}
				}
				if (buffer.length() > 0) {
					try {
						Integer val = Integer.parseInt(buffer.toString());
						parts.add(val);
					}
					catch (Exception e) {
						// not an int - just add the string
						parts.add(buffer.toString());
					}
				}
				log.debug("Version {} - PARTS:", value);
				for (Object obj : parts) {
					log.debug("   " + obj.toString());
				}
			}

			@SuppressWarnings({ "unchecked", "cast", "rawtypes" })
			@Override
			public int compareTo(Version o) {
				log.info("Comparing parts");
				for (int i = 0; i < parts.size(); i++) {
					if (i >= o.parts.size()) return -1;

					Object mine = parts.get(i);
					Object theirs = o.parts.get(i);
					int result;
					if (mine.getClass().isInstance(theirs)) {
						result = ((Comparable) mine).compareTo(theirs);
					}
					else {
						// We've hit something where one version is like: 17.20-RC-20171025.155355-1
						// and version 2 is: 17.20-20171024.222631-7
						// We are comparing RC to 20171024. By definition RC always ranks higher than a dated version.
						// This is, obviously, assuming we don't have other strings in the versions.
						if (mine instanceof String) {
							// theirs is an Integer
							result = 1;
						}
						else {
							// mine's an Integer, theirs is a string
							result = -1;
						}
					}

					if (result != 0) {
						log.info("...{} <> {} == {}", mine, theirs, result);
						return result;
					}
				}

				// if there are more parts in 'theirs' then return -1
				if (o.parts.size() > parts.size()) return -1;

				return 0;
			}
		}

		@Override
		public int compare(RundeckXO o1, RundeckXO o2) {
			Version v1 = new Version(o1.getValue());
			Version v2 = new Version(o2.getValue());
			return v2.compareTo(v1);
		}

	}

	private String latestVersion(String repositoryName, String groupId, String artifactId, String classifier,
			String extension) {
		List<RundeckXO> latestVersion = version(1, repositoryName, groupId, artifactId, classifier, extension);
		if (!latestVersion.isEmpty()) {
			return latestVersion.get(0).getValue();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private RundeckXO his2RundeckXO(SearchHit hit) {
		String version = (String) hit.getSource().get("version");

		List<Map<String, Object>> assets = (List<Map<String, Object>>) hit.getSource().get("assets");
		Map<String, Object> attributes = (Map<String, Object>) assets.get(0).get("attributes");
		Map<String, Object> content = (Map<String, Object>) attributes.get("content");
		String lastModifiedTime = "null";
		if (content != null && content.containsKey("last_modified")) {
			Long lastModified = (Long) content.get("last_modified");
			lastModifiedTime = DateUtils.formatDate(new Date(lastModified), "yyyy-MM-dd HH:mm:ss");
		}

		return RundeckXO.builder().name(version + " (" + lastModifiedTime + ")").value(version).build();
	}

	private Response commitAndReturn(Response response, StorageTx tx) {
		if (tx.isActive()) {
			tx.commit();
		}
		return response;
	}

}
