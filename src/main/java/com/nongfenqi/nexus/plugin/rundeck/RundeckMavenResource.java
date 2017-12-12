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
import static javax.ws.rs.core.MediaType.*;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.sonatype.nexus.common.text.Strings2.*;

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
	}

	@GET
	@Path("content")
	public Response content(@QueryParam("r") String repositoryName, @QueryParam("g") String groupId,
			@QueryParam("a") String artifactId, @QueryParam("v") String version, @QueryParam("c") String classifier,
			@QueryParam("p") @DefaultValue("jar") String extension) {

		// default version
		version = Optional.ofNullable(version)
				.orElse(latestVersion(repositoryName, groupId, artifactId, classifier, extension));

		// valid params
		if (isBlank(repositoryName) || isBlank(groupId) || isBlank(artifactId) || isBlank(version)) {
			return NOT_FOUND;
		}

		Repository repository = repositoryManager.get(repositoryName);
		if (null == repository || !repository.getFormat().getValue().equals("maven2")) {
			return NOT_FOUND;
		}

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

		String fileName = artifactId + "-" + version + (isBlank(classifier) ? "" : ("-" + classifier)) + "."
				+ extension;
		String path = groupId.replace(".", "/") + "/" + artifactId + "/" + version + "/" + fileName;
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
	public List<RundeckXO> version(@DefaultValue("10") @QueryParam("l") int limit, @QueryParam("r") String repository,
			@QueryParam("g") String groupId, @QueryParam("a") String artifactId, @QueryParam("c") String classifier,
			@QueryParam("p") String extension) {

		log.debug("param value, repository: {}, limit: {}, groupId: {}, artifactId: {}, classifier: {}, extension: {}",
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
		SearchResponse result = searchService.search(query,
				Collections.singletonList(
						new FieldSortBuilder("assets.attributes.content.last_modified").order(SortOrder.DESC)),
				0, limit);
		log.debug("Result: {}", result);
		List<RundeckXO> results = Arrays.stream(result.getHits().hits()).map(this::his2RundeckXO)
				.collect(Collectors.toList());

		// Another sort by version number
		Collections.sort(results, new VersionComparator());
		return results;
	}

	class VersionComparator implements Comparator<RundeckXO> {
		class Version implements Comparable<Version> {
			int major;
			int minor;
			int patch;
			String dateStr;

			public Version(String value) {
				major = 0;
				minor = 0;
				patch = 0;
				dateStr = "";

				// Expected formats:
				// 1.0-20170105.185744
				// 1.0.2-20170105.185744
				// 0.0.0-50-develop
				// 17.17.2
				try {
					String[] parts = value.split("\\.");

					log.debug("Parsing: {} - {}",value, parts[0]);

					major = Integer.parseInt(parts[0]);
					if (parts[1].indexOf("-") > 0) {
						String[] minorParts = parts[1].split("\\-");
						minor = Integer.parseInt(minorParts[0]);
						dateStr = minorParts[1];
					}
					else {
						minor = Integer.parseInt(parts[1]);

						if (parts.length >= 3) {
							if (parts[2].indexOf("-") > 0) {
								String[] patchParts = parts[2].split("\\-");
								patch = Integer.parseInt(patchParts[0]);
								dateStr = patchParts[1];
							}
							else {
								patch = Integer.parseInt(parts[2]);
							}
						}
						dateStr = "";
					}
				}
				catch (Exception e) {
					log.error("Could not parse: {}", value, e);
				}
			}

			public int compareTo(Version o) {
				if (major > o.major) return 1;
				if (major < o.major) return -1;

				if (minor > o.minor) return 1;
				if (minor < o.minor) return -1;

				if (patch > o.patch) return 1;
				if (patch < o.patch) return -1;

				return dateStr.compareTo(o.dateStr);
			}
		}

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
