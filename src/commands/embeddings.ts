import { buildCommand } from "@stricli/core";
import type { AppContext } from "../context.ts";
import { formatVector, Post } from "../util/db.ts";
import { extractEmbeddings, loadEmbeddingsModel } from "../util/embeddings.ts";
import { ProgressTracker } from "../util/progress.ts";
import { BackgroundQueue } from "../util/queue.ts";

export const embeddingsCommand = buildCommand({
	func: embeddingsCommandImpl,
	parameters: {
		flags: {
			force: {
				kind: "boolean",
				brief: "whether to overwrite existing embeddings",
				default: false,
			},
		},
	},
	docs: { brief: "generate embeddings for all posts you might've seen" },
});

async function embeddingsCommandImpl(this: AppContext, { force }: { force: boolean }) {
	console.log("loading embeddings model...");
	await loadEmbeddingsModel();

	console.log("calculating post count...");
	let postsCountQb = this.db.db.selectFrom("post").select((eb) =>
		eb.fn.countAll<number>().as("count")
	);
	if (!force) {
		postsCountQb = postsCountQb.where((eb) =>
			eb("embedding", "is", null).or("altTextEmbedding", "is", null)
		);
	}
	const { count: postsCount } = await postsCountQb.executeTakeFirstOrThrow();

	let offset = 0;
	let postsToEmbedQb = this.db.db.selectFrom("post").select([
		"creator",
		"rkey",
		"text",
		"altText",
	]);
	if (!force) {
		postsToEmbedQb = postsToEmbedQb.where((eb) =>
			eb("embedding", "is", null).or("altTextEmbedding", "is", null)
		);
	}
	postsToEmbedQb = postsToEmbedQb.limit(25).orderBy("creator", "asc").orderBy("rkey", "asc");

	const writeQueue = new BackgroundQueue(
		(post: Partial<Post>) =>
			this.db.db.updateTable("post").set({
				embedding: post.embedding && formatVector(post.embedding),
				altTextEmbedding: post.altTextEmbedding && formatVector(post.altTextEmbedding),
			}).where("creator", "=", post.creator!).where("rkey", "=", post.rkey!).execute(),
		{ hardConcurrency: 1 },
	);
	writeQueue.on("error", (e) => console.error(`error while writing post: ${e}`));

	console.log(`generating embeddings for ${postsCount} posts`);

	const progress = new ProgressTracker();
	using _logging = progress.start();
	progress.setTotal(postsCount);

	while (true) {
		const posts = await postsToEmbedQb.offset(offset).execute();
		offset += posts.length;

		if (posts.length === 0) break;

		const postIndexToText = new Map<number, number>();
		const nonEmptyTexts: string[] = [];
		posts.forEach((post, i) => {
			if (post.text.length > 0) {
				postIndexToText.set(i, nonEmptyTexts.length);
				nonEmptyTexts.push(post.text);
			}
		});

		const postIndexToAltText = new Map<number, number>();
		const nonEmptyAltTexts: string[] = [];
		posts.forEach((post, i) => {
			if (post.altText && post.altText.length > 0) {
				postIndexToAltText.set(i, nonEmptyAltTexts.length);
				nonEmptyAltTexts.push(post.altText);
			}
		});

		const [textEmbeddings, altTextEmbeddings] = await Promise.allSettled([
			nonEmptyTexts.length > 0 ? extractEmbeddings(nonEmptyTexts) : [],
			nonEmptyAltTexts.length > 0 ? extractEmbeddings(nonEmptyAltTexts) : [],
		]);
		if (textEmbeddings.status === "rejected") {
			console.error("failed to extract embeddings for texts:", textEmbeddings.reason);
			continue;
		}
		if (altTextEmbeddings.status === "rejected") {
			console.error("failed to extract embeddings for alt texts:", altTextEmbeddings.reason);
			continue;
		}

		posts.forEach((post, i) => {
			progress.incrementCompleted();
			if (!post.text.length && !post.altText?.length) return;

			const textEmbedding = postIndexToText.has(i)
				? textEmbeddings.value[postIndexToText.get(i)!]
				: null;
			const altTextEmbedding = postIndexToAltText.has(i)
				? altTextEmbeddings.value[postIndexToAltText.get(i)!]
				: null;

			if (!textEmbedding && !altTextEmbedding) return;

			void writeQueue.add({
				creator: post.creator,
				rkey: post.rkey,
				embedding: textEmbedding,
				altTextEmbedding: altTextEmbedding,
			});
		});
	}

	console.log("writing to database...");
	await writeQueue.processAll();

	console.log("embeddings generated!");
}
