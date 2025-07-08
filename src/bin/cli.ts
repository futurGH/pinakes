#!/usr/bin/env node
import { run } from "@stricli/core";
import { app } from "../app.ts";
import { buildContext } from "../context.ts";
await run(app, process.argv.slice(2), await buildContext());
