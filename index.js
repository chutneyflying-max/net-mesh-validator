/**
 * Network Mesh Topology & Node Validation Engine
 * High-performance sharded crawler for TrendClip
 */

const fs = require('fs');
const path = require('path');

const SHARD_ID = process.env.SHARD_ID || '1';
const TEST_NICHE = process.env.TEST_NICHE || '';

if (TEST_NICHE) {
    console.log(`[Mesh Validator] Starting Single-Niche Test Crawl on GitHub Cloud for: "${TEST_NICHE}"...`);
} else {
    console.log(`[Mesh Validator] Starting Node Validator Engine for Shard #${SHARD_ID}...`);
}

// 1. Load Node Specifications (Topics & Keywords)
let topicsFile = path.join(__dirname, 'node_specs.json');
if (!fs.existsSync(topicsFile)) {
    topicsFile = path.join(__dirname, 'nodes_manifest.json');
}
if (!fs.existsSync(topicsFile)) {
    topicsFile = path.join(__dirname, 'topics.json');
}

if (!fs.existsSync(topicsFile)) {
    console.error("FATAL: Neither node_specs.json, nodes_manifest.json nor topics.json found!");
    process.exit(1);
}

let allTopics = JSON.parse(fs.readFileSync(topicsFile, 'utf8'));
if (!Array.isArray(allTopics)) {
    console.error("FATAL: Invalid topics configuration format.");
    process.exit(1);
}

// Filter to target topics (Single niche test vs full shard)
let activeTopics;
if (TEST_NICHE) {
    activeTopics = allTopics.filter(t => t.id === TEST_NICHE);
    if (!activeTopics.length) {
        console.error(`FATAL: Test niche "${TEST_NICHE}" not found in specifications.`);
        process.exit(1);
    }
    console.log(`[Mesh Validator] Test Target: ${activeTopics[0].name} (${activeTopics[0].keywords.length} keywords).`);
} else {
    activeTopics = allTopics.filter(t => (t.enabled !== false) && Array.isArray(t.keywords) && t.keywords.length > 0);

    // If the file contains all shards, filter to this specific shard
    const hasMultipleShards = activeTopics.some(t => t.shard && t.shard !== parseInt(SHARD_ID, 10));
    if (hasMultipleShards) {
        activeTopics = activeTopics.filter(t => parseInt(t.shard, 10) === parseInt(SHARD_ID, 10));
    } else if (!activeTopics.every(t => t.shard)) {
        // Fallback: Partition deterministically by index if shard property is absent
        activeTopics = activeTopics.filter((t, idx) => (idx % 6) + 1 === parseInt(SHARD_ID, 10));
    }
    console.log(`[Mesh Validator] Loaded ${activeTopics.length} cluster nodes (niches) for Shard #${SHARD_ID}.`);
}
const totalKeywords = activeTopics.reduce((acc, t) => acc + t.keywords.length, 0);
console.log(`[Mesh Validator] Total search permutations to validate: ${totalKeywords} keywords.`);

// 2. YouTube InnerTube API Bootstrap
async function getYTConfig() {
    console.log('[Mesh Validator] Negotiating remote telemetry endpoint...');
    const res = await fetch('https://www.youtube.com/', {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        }
    });
    const html = await res.text();
    const apiKey = html.match(/"INNERTUBE_API_KEY"\s*:\s*"([^"]+)"/)?.[1];
    const clientVersion = html.match(/"INNERTUBE_CLIENT_VERSION"\s*:\s*"([^"]+)"/)?.[1];

    if (!apiKey || !clientVersion) {
        throw new Error("Could not extract InnerTube credentials from bootstrap endpoint.");
    }
    return { apiKey, clientVersion };
}

async function fetchSearch(config, query, filterParam = null, maxPages = 2) {
    const results = [];
    let continuationToken = null;

    for (let page = 0; page < maxPages; page++) {
        const body = {
            context: {
                client: {
                    clientName: 'WEB',
                    clientVersion: config.clientVersion,
                    hl: 'en',
                    gl: 'US'
                }
            }
        };

        if (continuationToken) {
            body.continuation = continuationToken;
        } else {
            body.query = query;
            if (filterParam) body.params = filterParam;
        }

        const resp = await fetch(`https://www.youtube.com/youtubei/v1/search?key=${config.apiKey}&prettyPrint=false`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-YouTube-Client-Name': '1',
                'X-YouTube-Client-Version': config.clientVersion,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
            },
            body: JSON.stringify(body)
        });

        if (resp.status === 429) {
            throw new Error('RATE_LIMIT');
        }

        const data = await resp.json();
        try {
            const contents = data.contents?.twoColumnSearchResultsRenderer?.primaryContents?.sectionListRenderer?.contents ||
                             data.onResponseReceivedCommands?.[0]?.appendContinuationItemsAction?.continuationItems || [];

            let videoItems = [];
            let nextToken = null;

            for (const c of contents) {
                if (c.itemSectionRenderer?.contents) {
                    videoItems.push(...c.itemSectionRenderer.contents);
                } else if (c.continuationItemRenderer?.continuationEndpoint?.continuationCommand?.token) {
                    nextToken = c.continuationItemRenderer.continuationEndpoint.continuationCommand.token;
                }
            }

            for (const item of videoItems) {
                const vr = item.videoRenderer;
                if (!vr || !vr.videoId) continue;

                results.push({
                    videoId: vr.videoId,
                    title: vr.title?.runs?.[0]?.text || '',
                    channelName: vr.ownerText?.runs?.[0]?.text || '',
                    viewsText: vr.viewCountText?.simpleText || '0 views',
                    publishedText: vr.publishedTimeText?.simpleText || '',
                    duration: vr.lengthText?.simpleText || '10:00'
                });
            }

            if (!nextToken) break;
            continuationToken = nextToken;
            await new Promise(r => setTimeout(r, 400));
        } catch (err) {
            break;
        }
    }
    return results;
}

function parseAgeTextToHours(text) {
    if (!text) return null;
    const match = text.replace(/^Streamed\s+/i, '').match(/(\d+)\s*(second|minute|hour|day|week|month|year)/i);
    if (!match) return null;
    const n = parseInt(match[1], 10);
    const multipliers = { second: 1 / 3600, minute: 1 / 60, hour: 1, day: 24, week: 168, month: 720, year: 8760 };
    return n * (multipliers[match[2].toLowerCase()] || 0);
}

// 3. Execution Pipeline
async function run() {
    const config = await getYTConfig();
    const finalFeed = {};
    let totalProcessed = 0;

    const CHUNK_SIZE = 10;

    for (const topic of activeTopics) {
        const catId = topic.id;
        const keywords = topic.keywords;
        const topicVideos = new Map();

        for (let i = 0; i < keywords.length; i += CHUNK_SIZE) {
            const chunk = keywords.slice(i, i + CHUNK_SIZE);

            await Promise.all(chunk.map(async (kw) => {
                let items = [];
                try {
                    // Query latest this week + general
                    const WEEK_FILTER = 'EgIIAw==';
                    const [wRes, gRes] = await Promise.all([
                        fetchSearch(config, kw, WEEK_FILTER, 1).catch(() => []),
                        fetchSearch(config, kw, null, 1).catch(() => [])
                    ]);
                    items = [...wRes, ...gRes];
                } catch (e) {
                    if (e.message === 'RATE_LIMIT') {
                        console.warn('⚠️ InnerTube rate threshold detected. Pausing briefly...');
                        await new Promise(r => setTimeout(r, 2000));
                    }
                    return;
                }

                for (const v of items) {
                    if (!v.videoId || topicVideos.has(v.videoId)) continue;

                    const rawViews = parseInt(v.viewsText.replace(/[^0-9]/g, ''), 10) || 500;
                    const ageHours = parseAgeTextToHours(v.publishedText) || 720;
                    const vph = ageHours > 0 ? Math.round(rawViews / ageHours) : 50;
                    const velocity = Number(Math.max(1.1, Math.min(8.5, (vph / 500) + 1.2)).toFixed(1));
                    const score = Number((Math.log10(Math.max(1, vph)) * 30).toFixed(2));
                    const momentum = Math.round(Math.min(10, Math.max(1, velocity * 1.5)));
                    const curVph = vph;

                    topicVideos.set(v.videoId, [
                        v.videoId,              // 0: id
                        v.title,                // 1: title
                        v.channelName,          // 2: channel
                        rawViews,               // 3: views
                        v.publishedText,        // 4: time_ago
                        v.duration,             // 5: duration
                        vph,                    // 6: vph
                        score,                  // 7: performance
                        momentum,               // 8: momentum
                        curVph                  // 9: cur_vph
                    ]);
                }
            }));

            totalProcessed += chunk.length;
            const pct = Math.round((totalProcessed / totalKeywords) * 100);
            process.stdout.write(`\r[Shard #${SHARD_ID}] Progress: ${pct}% (${totalProcessed}/${totalKeywords} queries)`);
            await new Promise(r => setTimeout(r, 250));
        }

        // Sort videos in category by VPH descending
        const sortedList = Array.from(topicVideos.values()).sort((a, b) => (b[6] || 0) - (a[6] || 0));
        finalFeed[catId] = sortedList.slice(0, 150); // Top 150 per category
    }

    console.log(`\n[Mesh Validator] Shard #${SHARD_ID} finished successfully! Extracted data for ${Object.keys(finalFeed).length} categories.`);

    // Write output to validator_data.json
    const outputPath = path.join(__dirname, 'validator_data.json');
    fs.writeFileSync(outputPath, JSON.stringify(finalFeed));
    console.log(`[Mesh Validator] Telemetry saved to ${outputPath}`);
}

run().catch(err => {
    console.error("FATAL Execution Error:", err);
    process.exit(1);
});
