#!/usr/bin/env node
import { run } from "@stricli/core";
import { buildContext } from "../context.ts";
import { app } from "../app.ts";
await run(app, Deno.args, await buildContext());
