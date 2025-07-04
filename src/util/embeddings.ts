import { type FeatureExtractionPipeline, pipeline } from "@huggingface/transformers";

let extractor: FeatureExtractionPipeline | undefined;

export async function loadEmbeddingsModel() {
	try {
		return extractor ??= await pipeline("feature-extraction", "Xenova/multi-qa-MiniLM-L6-cos-v1", {
			device: navigator.gpu ? "webgpu" : "cpu",
			dtype: "auto",
		});
	} catch {
		return extractor ??= await pipeline("feature-extraction", "Xenova/multi-qa-MiniLM-L6-cos-v1", {
			device: "cpu",
			dtype: "auto",
		});
	}
}

export async function extractEmbeddings(text: string): Promise<Float32Array>;
export async function extractEmbeddings(texts: string[]): Promise<Float32Array[]>;
export async function extractEmbeddings(
	texts: string | string[],
): Promise<Float32Array | Float32Array[]> {
	const extractor = await loadEmbeddingsModel();
	const res = await extractor(texts, { pooling: "mean", normalize: true });
	return Array.isArray(texts)
		? res.tolist().map((arr) => Float32Array.from(arr))
		: Float32Array.from(res.data);
}
