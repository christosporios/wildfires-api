import { Announcement, Coordinates, DataSourceData, TimedEvent, Wildfire } from "../types";
import DataSource from "./dataSource";
import { SearchMode, Tweet, Scraper as TwitterScraper } from "@the-convocation/twitter-scraper";
import dotenv from 'dotenv';
import Anthropic from "@anthropic-ai/sdk";
import axios from 'axios';
const cld = require('cld');

dotenv.config({ path: '.env.local' });
const anthropic = new Anthropic();

export default class Announcements extends DataSource {
    private bounds: [Coordinates, Coordinates];
    private scraper: TwitterScraper | null = null;

    constructor(wildfire: Wildfire) {
        super(wildfire.id, "announcements");
        this.bounds = wildfire.boundingBox;
    }

    getMeta(): any {
        return {
            bounds: this.bounds
        };
    }

    initFromWildfire(wildfire: Wildfire): void {
        this.bounds = wildfire.boundingBox;
    }

    initFromSavedData(data: DataSourceData): void {
        this.bounds = data.meta.bounds;
    }

    isFullFetchNeeded(wildfire: Wildfire): boolean {
        // Compare the current bounds with the wildfire's bounding box
        return this.bounds.some((currentBound, index) =>
            currentBound.some((coord, coordIndex) =>
                Math.abs(coord - wildfire.boundingBox[index][coordIndex]) > Number.EPSILON
            )
        );
    }

    private async initLoggedInScraper(): Promise<TwitterScraper> {
        if (this.scraper) {
            return this.scraper;
        }

        const s = new TwitterScraper();
        try {
            await s.login(process.env.TWITTER_USERNAME!, process.env.TWITTER_PASSWORD!);
            this.scraper = s;
        } catch (e) {
            console.error("Failed to login to Twitter", e);
            throw e;
        }
        return s;
    }

    async fetchInterval(from: Date, to: Date): Promise<TimedEvent[]> {
        const tweets = await this.fetchTweets(from, to);

        const greekTweets = await Promise.all(tweets.map(async tweet => {
            const detectedLanguage = (await cld.detect(tweet.text!)).languages[0].code
            return detectedLanguage === "el" ? tweet : null;
        })).then(results => results.filter(tweet => tweet !== null)) as Tweet[];

        const greekPercentage = (greekTweets.length / tweets.length) * 100;
        if (greekPercentage < 30) {
            this.log(`Warning: Only ${greekPercentage.toFixed(2)}% of tweets are in Greek. This may indicate an issue with language detection or data source.`);
        } else {
            console.log(`Of ${tweets.length} tweets, ${greekTweets.length} greek`);
        }

        const parsedTweets: { type?: "alert" | "evacuate", from: string[], to?: string[], tweetUrl: string, timestamp: number }[] = [];

        for (const tweet of greekTweets) {
            const parsed = await this.parseTweet(tweet.text!);
            if (parsed) {
                parsedTweets.push({
                    ...parsed,
                    tweetUrl: `https://twitter.com/112Greece/status/${tweet.id}`,
                    timestamp: tweet.timestamp!
                });
            }

            // Wait 1200ms between parsing tweets
            await new Promise(resolve => setTimeout(resolve, 1200));
        }

        parsedTweets.filter(tweet => tweet.type && tweet.from && ["evacuate", "alert"].includes(tweet.type));
        this.log(`Parsed ${parsedTweets.length} tweets`);

        const announcements: Announcement[] = await Promise.all(parsedTweets.map(async (tweet) => {
            const getPositions = async (names: string[]) => {
                if (!names || names.length === 0) {
                    return [];
                }
                const positions = await Promise.all(names.map(async name => {
                    const position = await this.getCoordinates(this.bounds, name);
                    return position ? { name, position } : null;
                }));
                return positions.filter(pos => pos !== null);
            };

            const fromPositions = await getPositions(tweet.from);
            const toPositions = tweet.to ? await getPositions(tweet.to) : undefined;

            return {
                event: "announcement",
                type: tweet.type,
                tweetUrl: tweet.tweetUrl,
                timestamp: tweet.timestamp,
                from: fromPositions,
                to: toPositions
            } as Announcement;
        }));

        return announcements;
    }

    private async fetchTweets(from: Date, to: Date): Promise<Tweet[]> {
        const s = await this.initLoggedInScraper();
        this.log(`Fetching tweets from ${from} to ${to}`);

        let twitterSearchQuery = `(from:112Greece) until:${to.toISOString().slice(0, 10)} since:${from.toISOString().slice(0, 10)}`;
        let tweetsGen = await s.searchTweets(twitterSearchQuery, 500, SearchMode.Latest);
        let tweets = await s.getTweetsWhere(tweetsGen, (t) => true);

        /*
        const allTweets = await s.getTweets("112Greece");
        const tweets = await s.getTweetsWhere(allTweets, (t) => t.timestamp! * 1000 > from.getTime() && t.timestamp! * 1000 < to.getTime());
        */

        this.log(`Fetched ${tweets.length} tweets`);
        return tweets;
    }

    private async parseTweet(tweetText: string): Promise<{ type: "alert" | "evacuate", from: string[], to?: string[] } | null> {
        let msg;
        try {
            msg = await anthropic.messages.create({
                model: "claude-3-5-sonnet-20240620",
                max_tokens: 1000,
                temperature: 0,
                messages: [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": `You are tasked with extracting location information from Greek-language tweets posted by @112Greece, an emergency number account. You will be provided with a tweet text, and your goal is to identify the type of message and extract relevant location information. Here's the tweet text you'll be working with:\n\n<tweet_text>\n${tweetText}\n</tweet_text>\n\nFollow these steps to analyze the tweet and extract the required information:\n\n1. Determine if the tweet is in Greek. If it's in English, respond with an empty JSON object: {}\n\n2. Identify the type of message:\n   - \"alert\": A warning for specific areas\n   - \"evacuation\": Instructions to evacuate from one area to another\n\n3. Extract locations:\n   - For \"alert\" type: Identify the areas being alerted\n   - For \"evacuation\" type: Identify the areas to evacuate from (\"from\") and the areas to evacuate to (\"to\")\n\n4. Pay attention to special cases:\n   - Ignore routes or streets mentioned (e.g., \"via Dionysos Avenue\")\n   - If a location is specified with a wider region, combine them (e.g., \"Stamata, Attica\")\n\n5. Format the extracted information as a JSON object with the following structure:\n   {\n     \"type\": \"evacuation\" or \"alert\",\n     \"from\": [array of strings],\n     \"to\": [array of strings]\n   }\n\n   Note: For \"alert\" type, the \"to\" field should be an empty array.\n\n6. Remove the \"#\" symbol from location names and replace underscores with spaces.\n\nHere are some examples to guide you:\n\nExample 1:\nTweet: \"⚠️ Activation of 112 - Emergency Number 🔥 Wildfire near your area 🆘 If you are in the area of #Anatoli evacuate towards #Nea_Makri ‼️ Follow the instructions of the Authorities\"\nOutput:\n{\n  \"type\": \"evacuation\",\n  \"from\": [\"Anatoli\"],\n  \"to\": [\"Nea Makri\"]\n}\n\nExample 2:\nTweet: \"⚠️ Activation of 112 - Emergency Number 🔥 Wildfire near the areas of #Stamata, #Rodopoli, #Agio_Stefano, and #Dionisos of #Attica ❗ Stay alert ‼️ Follow the instructions of the Authorities\"\nOutput:\n{\n  \"type\": \"alert\",\n  \"from\": [\"Stamata, Attica\", \"Rodopoli, Attica\", \"Agio Stefano, Attica\", \"Dionisos, Attica\"],\n  \"to\": []\n}\n\nExample 3:\nTweet: \"\"⚠️ Activation of 112 - Emergency Number\n\n🔥 Wildfire in your area\n\n🆘 If you are in the areas of #Agia_Kyriaki, #Kastrí #Evia, evacuate towards #Eretria.\n\n‼️ Follow the instructions of the Authorities\n\nℹ️ https://t.co/tg45OiBehz\n\n@pyrosvestiki\n@hellenicpolice\"\"\nOutput:\n{\n  type: \"evacuation\",\n  from: [\"Agia Kyriaki\", \"Kastri, Evia\"]\n  to: [\"Eretria\"]\n}\n\nRemember to carefully analyze the tweet text, extract the relevant information, and format your response as a JSON object. If you're unsure about any part of the tweet or cannot extract the required information, provide the best possible interpretation based on the given instructions.\n\nRESPOND ONLY WITH JSON, NOTHING ELSE.`
                            }
                        ]
                    }
                ]
            });
        } catch (e) {
            this.log(`Error parsing tweet with anthropic: ${e}`);
            return null;
        }

        const json = JSON.parse(msg.content[0].type === 'text' ? msg.content[0].text : "");
        this.log(`Parsed tweet: ${JSON.stringify(json)}`);
        return json;
    }

    private async getCoordinates(bounds: [Coordinates, Coordinates], name: string): Promise<Coordinates | null> {
        try {
            const [[lat1, lon1], [lat2, lon2]] = bounds;
            const minLat = Math.min(lat1, lat2);
            const maxLat = Math.max(lat1, lat2);
            const minLon = Math.min(lon1, lon2);
            const maxLon = Math.max(lon1, lon2);

            const response = await axios.get('https://maps.googleapis.com/maps/api/geocode/json', {
                params: {
                    address: `${name}, Greece`,
                    key: process.env.GOOGLE_API_KEY
                }
            });

            if (response.data.results.length > 0) {
                for (let i = 0; i < response.data.results.length; i++) {
                    const { lat, lng } = response.data.results[i].geometry.location;
                    if (lat >= minLat && lat <= maxLat && lng >= minLon && lng <= maxLon) {
                        if (i > 0) {
                            this.log(`Did not pick first result for ${name}, because it was out of bounds. Result #${i + 1} was within bounds.`);
                        }
                        this.log(`Coordinates for ${name}: ${lat}, ${lng}`);
                        return [lat, lng];
                    }
                }
                this.log(`No result was within bounds for ${name}`);
                const { lat, lng } = response.data.results[0].geometry.location;
                this.log(`Coordinates for ${name}: ${lat}, ${lng}`);
                return [lat, lng];
            } else {
                this.log(`No results found for ${name}`);
                return null;
            }
        } catch (error) {
            this.log(`Error getting coordinates for ${name}: ${error}`);
            return null;
        }
    }
}
