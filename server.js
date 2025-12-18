import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { Pool } from "pg";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, "data");
const HEADERS_FILE = path.join(DATA_DIR, "headers.json");

const API_URL = "https://api-partner.spotify.com/pathfinder/v2/query";

const QUERY_SHOW_METADATA_HASH =
  "26d0c98fef216dad02d31c359075c07d605974af8d82834f26e90f917f32555a";

const SCRAPE_NINJA_ENDPOINT = "https://scrapeninja.p.rapidapi.com/scrape";
const SCRAPE_NINJA_HOST = "scrapeninja.p.rapidapi.com";
const DEFAULT_SCRAPE_NINJA_API_KEY =
  "455e2a6556msheffc310f7420b51p102ea0jsn1c531be1e299";
const SCRAPE_NINJA_API_KEY =
  process.env.SCRAPE_NINJA_API_KEY || DEFAULT_SCRAPE_NINJA_API_KEY;
const USE_SCRAPE_NINJA = process.env.USE_SCRAPE_NINJA === "true";

const DB_CONFIG = {
  host: process.env.DB_HOST || "3.140.167.34",
  port: Number.parseInt(process.env.DB_PORT, 10) || 5432,
  user: process.env.DB_USER || "redash",
  password: process.env.DB_PASSWORD || "te83NECug38ueP",
  database: process.env.DB_NAME || "scrapers",
};

const FETCH_PROFILE_URLS_SQL =
  "select url, search_id from spotify.not_scraped_profiles_vw";
const INSERT_PROFILE_SQL =
  "insert into spotify.profiles(show_name, host_name, about, rate, reviews, url, links, category, search_id, episode_description) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) on conflict (url) do nothing";

function buildAuthorizationHeader(value) {
  if (!value || typeof value !== "string") {
    return "";
  }

  const trimmed = value.trim();
  return trimmed.toLowerCase().startsWith("bearer")
    ? trimmed
    : `Bearer ${trimmed}`;
}

const DEFAULT_HEADERS = {
  accept: "application/json",
  "accept-language": "en",
  "app-platform": "WebPlayer",
  authorization: buildAuthorizationHeader(process.env.SPOTIFY_AUTHORIZATION),
  "client-token": process.env.SPOTIFY_CLIENT_TOKEN?.trim() || "",
  "content-type": "application/json;charset=UTF-8",
  origin: "https://open.spotify.com",
  priority: "u=1, i",
  referer: "https://open.spotify.com/",
  "sec-ch-ua":
    '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"Windows"',
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-site",
  "spotify-app-version": "1.2.78.120.g186ece09",
  "user-agent":
    process.env.USER_AGENT ||
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
};
async function ensureDataDir() {
  await fs.mkdir(DATA_DIR, { recursive: true });
}

async function loadHeaderOverrides() {
  try {
    const raw = await fs.readFile(HEADERS_FILE, "utf-8");
    const parsed = JSON.parse(raw);

    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }

    return parsed;
  } catch (error) {
    return {};
  }
}

function buildRequestHeaders(overrides) {
  const headers = { ...DEFAULT_HEADERS };

  Object.entries(overrides || {}).forEach(([key, value]) => {
    if (typeof value === "string" && value.trim() !== "") {
      headers[key.toLowerCase()] = value.trim();
    }
  });

  return Object.fromEntries(
    Object.entries(headers).map(([key, value]) => [key, value]).filter(([, value]) => value !== "")
  );
}

function validateAuthHeaders(headers) {
  const missing = [];

  if (!headers.authorization) {
    missing.push(
      "authorization (Bearer token). Supply SPOTIFY_AUTHORIZATION env var or data/headers.json"
    );
  }

  if (!headers["client-token"]) {
    missing.push(
      "client-token. Supply SPOTIFY_CLIENT_TOKEN env var or data/headers.json"
    );
  }

  if (missing.length) {
    throw new Error(
      `Missing required Spotify auth headers: ${missing.join("; ")}. Requests will fail with 401 until these are provided.`
    );
  }
}

function buildUriFromUrl(url) {
  try {
    const parsed = new URL(url);
    const segments = parsed.pathname.split("/").filter(Boolean);

    if (segments.length < 2) {
      return null;
    }

    const [resourceType, resourceId] = segments;
    return `spotify:${resourceType}:${resourceId}`;
  } catch (error) {
    return null;
  }
}

function skipDomains(url) {
  const EXCLUDED_DOMAINS = ["patreon.com", "speaker.com"];

  try {
    const hostname = new URL(url).hostname.toLowerCase();

    return EXCLUDED_DOMAINS.some(
      (domain) =>
        hostname === domain || hostname.endsWith(`.${domain}`)
    );
  } catch (error) {
    return false;
  }
}

function extractLinksFromDescription(description) {
  if (typeof description !== "string" || description.trim() === "") {
    return "";
  }

  const urlRegex = /https?:\/\/[^\s"'<>]+/gi;
  const matches = description.match(urlRegex) || [];

  return matches
    .map((match) => match.trim())
    .filter((match) => Boolean(match) && !skipDomains(match))
    .join("◙");
}

function normalizeUri(uriOrUrl) {
  if (typeof uriOrUrl !== "string") {
    return null;
  }

  const trimmed = uriOrUrl.trim();

  if (trimmed === "") {
    return null;
  }

  if (trimmed.startsWith("spotify:")) {
    return trimmed;
  }

  return buildUriFromUrl(trimmed);
}

function buildShowRequestBody(uri) {
  const normalizedUri = normalizeUri(uri);

  if (!normalizedUri) {
    throw new Error(
      "Invalid Spotify show identifier. Provide a spotify: URI or an open.spotify.com URL."
    );
  }

  return {
    variables: { uri: normalizedUri },
    operationName: "queryShowMetadataV2",
    extensions: {
      persistedQuery: {
        version: 1,
        sha256Hash: QUERY_SHOW_METADATA_HASH,
      },
    },
  };
}

function buildEpisodeRequestBody(uri) {
  const normalizedUri = normalizeUri(uri);

  if (!normalizedUri) {
    throw new Error(
      "Invalid Spotify episode identifier. Provide a spotify: URI or an open.spotify.com URL."
    );
  }

  return {
    variables: { uri: normalizedUri },
    operationName: "getEpisodeDescription",
    query:
      "query getEpisodeDescription($uri: ID!) { episodeUnionV2(uri: $uri) { __typename ... on Episode { htmlDescription } ... on UnknownEpisode { htmlDescription } } }",
  };
}


function parseProfileResponse(responseJson) {
  assertNoGraphQlErrors(responseJson, "show metadata");

  const podcast = responseJson?.data?.podcastUnionV2;
  if (!podcast || typeof podcast !== "object") {
    return null;
  }

  const showName =
    typeof podcast?.name === "string" ? podcast.name.trim() : "";
  const hostName =
    typeof podcast?.publisher?.name === "string"
      ? podcast.publisher.name.trim()
      : "";
  const about =
    typeof podcast?.description === "string" ? podcast.description.trim() : "";

  const category = (() => {
    const topicItems = podcast?.topics?.items;

    if (Array.isArray(topicItems)) {
      return topicItems
        .map((topic) =>
          typeof topic?.title === "string" ? topic.title.trim() : ""
        )
        .filter(Boolean)
        .join(", ");
    }

    return "";
  })();


  const averageRating =
    typeof podcast?.rating?.averageRating?.average === "number"
      ? podcast.rating.averageRating.average
      : podcast?.rating?.average;

  const rate =
    typeof averageRating === "number" && Number.isFinite(averageRating)
      ? (Math.floor(averageRating * 10) / 10).toFixed(1)
      : "";

  const totalRatings =
    podcast?.rating?.averageRating?.totalRatings ?? podcast?.rating?.totalRatings;

  const reviews =
    typeof totalRatings === "number" && Number.isFinite(totalRatings)
      ? String(totalRatings)
      : typeof totalRatings === "string"
        ? totalRatings
        : "";

  if (!showName || !hostName) {
    return null;
  }

  return { showName, hostName, about, rate, reviews, category };
}

function extractEpisodeUris(responseJson) {
  const episodes =
    responseJson?.data?.podcastUnionV2?.episodesV2?.items || [];

  return episodes
    .map((episode) => {
      const uri = episode?.entity?.data?.uri;
      return typeof uri === "string" ? uri.trim() : "";
    })
    .filter((uri) => uri !== "");
}

function parseEpisodeDescription(responseJson) {
  assertNoGraphQlErrors(responseJson, "episode metadata");

  const rawDescription = responseJson?.data?.episodeUnionV2?.htmlDescription;

  if (typeof rawDescription !== "string") {
    return "";
  }

  return rawDescription.trim();
}

function assertNoGraphQlErrors(responseJson, context) {
  if (!Array.isArray(responseJson?.errors) || responseJson.errors.length === 0) {
    return;
  }

  const message = responseJson.errors
    .map((error) =>
      typeof error?.message === "string" ? error.message.trim() : ""
    )
    .filter(Boolean)
    .join("; ");

  if (/client is not defined/i.test(message)) {
    throw new Error(
      `Spotify GraphQL error while fetching ${context}: ${message}. This usually means the authorization or client-token headers are missing or expired. Update SPOTIFY_AUTHORIZATION and SPOTIFY_CLIENT_TOKEN (or data/headers.json).`
    );
  }

  throw new Error(
    message
      ? `Spotify GraphQL error while fetching ${context}: ${message}`
      : `Spotify API returned an error response while fetching ${context}.`
  );
}

async function postSpotifyRequest(headers, body) {
  if (USE_SCRAPE_NINJA) {
    const scrapeResponse = await fetch(SCRAPE_NINJA_ENDPOINT, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-rapidapi-host": SCRAPE_NINJA_HOST,
        "x-rapidapi-key": SCRAPE_NINJA_API_KEY,
      },
      body: JSON.stringify({
        url: API_URL,
        method: "POST",
        headers,
        body: JSON.stringify(body),
      }),
    });

    if (!scrapeResponse.ok) {
      const text = await scrapeResponse.text();
      throw new Error(
        `Scrape Ninja request failed with status ${scrapeResponse.status}: ${text.slice(0, 200)}`
      );
    }

    const result = await scrapeResponse.json();
    const parsedBody = result?.body ? JSON.parse(result.body) : null;

    if (!parsedBody) {
      throw new Error("Scrape Ninja response did not include a parsable body.");
    }

    return parsedBody;
  }

  const response = await fetch(API_URL, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(
      `Request failed with status ${response.status}: ${text.slice(0, 200)}`
    );
  }

  return response.json();
}

async function fetchShowMetadata(headers, uri) {
  return postSpotifyRequest(headers, buildShowRequestBody(uri));
}

async function fetchEpisodeMetadata(headers, uri) {
  return postSpotifyRequest(headers, buildEpisodeRequestBody(uri));
}

async function loadProfileUrls(pool) {
  const { rows } = await pool.query(FETCH_PROFILE_URLS_SQL);

  return rows
    .map((row) => ({
      url: row && typeof row.url === "string" ? row.url.trim() : "",
      searchId: (() => {
        const rawSearchId = row?.search_id;

        if (typeof rawSearchId === "string") {
          return rawSearchId.trim();
        }

        if (typeof rawSearchId === "number" && Number.isFinite(rawSearchId)) {
          return String(rawSearchId);
        }

        return "";
      })(),
    }))
    .filter((value) => value.url !== "");
}

async function saveProfile(pool, profile) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await client.query(INSERT_PROFILE_SQL, [
      profile.showName,
      profile.hostName,
      profile.about,
      profile.rate,
      profile.reviews,
      profile.url,
      profile.links,
      profile.category,
      profile.searchId || null,
      profile.episodeDescription || null,
    ]);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function ensureProfilesTableSchema(pool) {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    await client.query(
      "alter table if exists spotify.profiles add column if not exists url text"
    );
    await client.query(
      "alter table if exists spotify.profiles add column if not exists links text"
    );
    await client.query(
      "alter table if exists spotify.profiles add column if not exists category text"
    );
    await client.query(
      "alter table if exists spotify.profiles add column if not exists search_id text"
    );
    await client.query(
      "alter table if exists spotify.profiles add column if not exists episode_description text"
    );
    await client.query(
      "create unique index if not exists profiles_url_key on spotify.profiles(url)"
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw new Error(
      `Failed to ensure spotify.profiles schema is ready: ${error.message}`
    );
  } finally {
    client.release();
  }
}

async function main() {
  await ensureDataDir();

  const headerOverrides = await loadHeaderOverrides();
  const headers = buildRequestHeaders(headerOverrides);

  validateAuthHeaders(headers);

  const pool = new Pool(DB_CONFIG);

  try {
    await ensureProfilesTableSchema(pool);
    const profilesToProcess = await loadProfileUrls(pool);

    if (!profilesToProcess.length) {
      console.warn("No profiles found to process.");
      return;
    }

    console.log(
      `Processing ${profilesToProcess.length} profile${profilesToProcess.length === 1 ? "" : "s"}.`
    );

    for (const { url, searchId } of profilesToProcess) {
      try {
        const uri = buildUriFromUrl(url);

        if (!uri) {
          console.warn(`Could not build Spotify URI from URL: ${url}`);
          continue;
        }

        const responseJson = await fetchShowMetadata(headers, uri);
        const profile = parseProfileResponse(responseJson);

        if (!profile) {
          console.warn(`No profile data returned for URL: ${url}`);
          continue;
        }

        const episodeUris = extractEpisodeUris(responseJson);
        let episodeDescription = "";

        for (const episodeUri of episodeUris) {
          try {
            const episodeResponse = await fetchEpisodeMetadata(
              headers,
              episodeUri
            );

            const parsedDescription = parseEpisodeDescription(episodeResponse);

            if (parsedDescription) {
              episodeDescription = parsedDescription;
              break;
            }
          } catch (error) {
            console.warn(
              `Failed to fetch episode description for URI "${episodeUri}": ${error.message}`
            );
          }
        }

        const links = extractLinksFromDescription(profile.about);

        await saveProfile(pool, {
          ...profile,
          url,
          links,
          searchId,
          episodeDescription,
        });
        console.log(`Saved profile for URL "${url}".`);
      } catch (error) {
        console.error(`Failed to process URL "${url}": ${error.message}`);
      }
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error("Fatal error while running scraper:", error);
  process.exitCode = 1;
});
