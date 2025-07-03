import type { AppContext } from "../../context.ts";

interface FooCommandFlags {
	// ...
}

export async function foo(this: AppContext, flags: FooCommandFlags): Promise<void> {
	// ...
}

interface BarCommandFlags {
	// ...
}

export async function bar(this: AppContext, flags: BarCommandFlags): Promise<void> {
	// ...
}
