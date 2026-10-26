import { pathToFileURL } from "node:url";

const MIN_NODE_MAJOR = 20;
const EXTENSION_CONTEXTS = ["github", "inputs", "needs"];

function fail(message) {
  throw new Error(message);
}

function assertNodeVersion() {
  const major = Number(process.versions.node.split(".")[0]);
  if (major < MIN_NODE_MAJOR) {
    fail(`node>=${MIN_NODE_MAJOR} required; node=${process.versions.node}`);
  }
}

function toExpressionData(value, data) {
  if (value === null) return new data.Null();
  if (typeof value === "string") return new data.StringData(value);
  if (typeof value === "boolean") return new data.BooleanData(value);
  if (typeof value === "number") return new data.NumberData(value);
  if (Array.isArray(value)) {
    return new data.Array(...value.map((item) => toExpressionData(item, data)));
  }
  if (typeof value !== "object") fail(`unsupported context value: ${typeof value}`);
  return new data.Dictionary(
    ...Object.entries(value).map(([key, item]) => ({
      key,
      value: toExpressionData(item, data),
    })),
  );
}

function unwrapExpression(source) {
  const trimmed = source.trim();
  const starts = trimmed.startsWith("${{");
  const ends = trimmed.endsWith("}}");
  if (starts !== ends) fail("expression wrapper is incomplete");
  if (!starts) return trimmed;
  return trimmed.slice(3, -2).trim();
}

function extensionInfo(name) {
  return { name, minArgs: 0, maxArgs: 0 };
}

function unsupportedStatus(name) {
  return {
    ...extensionInfo(name),
    call: () => fail(`unsupported status function: ${name}; scheduler not modeled`),
  };
}

function normalizeFunctionName(name) {
  return name.toLowerCase();
}

function extensionFunctions(cancelled, data) {
  return new Map([
    ["always", { ...extensionInfo("always"), call: () => new data.BooleanData(true) }],
    [
      "cancelled",
      {
        ...extensionInfo("cancelled"),
        call: () => new data.BooleanData(cancelled),
      },
    ],
    ["success", unsupportedStatus("success")],
    ["failure", unsupportedStatus("failure")],
  ]);
}

function functionNames(tree) {
  const names = new Set();
  const visit = (node) => {
    if (!node || typeof node !== "object") return;
    if (node.functionName?.lexeme) {
      names.add(normalizeFunctionName(node.functionName.lexeme));
    }
    for (const value of Object.values(node)) visit(value);
  };
  visit(tree);
  return names;
}

function evaluateExpression(source, context, runtime) {
  const { api, cancelled, requireBoolean = true } = runtime;
  if (!context || Array.isArray(context) || typeof context !== "object") {
    fail("context must be an object");
  }
  const tokens = new api.Lexer(unwrapExpression(source)).lex().tokens;
  const parser = new api.Parser(
    tokens,
    EXTENSION_CONTEXTS,
    [
      extensionInfo("always"),
      extensionInfo("cancelled"),
      extensionInfo("success"),
      extensionInfo("failure"),
    ],
  );
  const tree = parser.parse();
  const statusFunctions = functionNames(tree);
  if (statusFunctions.has("success")) {
    fail("unsupported status function: success; scheduler not modeled");
  }
  if (statusFunctions.has("failure")) {
    fail("unsupported status function: failure; scheduler not modeled");
  }
  if (context.needs && !["always", "cancelled", "success", "failure"].some(
    (name) => statusFunctions.has(name),
  )) {
    fail("implicit success() is not modeled for jobs with needs");
  }
  const value = new api.Evaluator(
    tree,
    toExpressionData(context, api.data),
    extensionFunctions(cancelled, api.data),
  ).evaluate();
  if (requireBoolean && value.kind !== api.data.Kind.Boolean) {
    fail(`expression result must be boolean; kind=${value.kind}`);
  }
  return value.kind === api.data.Kind.Boolean ? value.value : value.coerceString();
}

function evaluateTemplate(source, context, runtime) {
  const pattern = /\$\{\{\s*([\s\S]*?)\s*\}\}/g;
  let cursor = 0;
  let result = "";
  let match;
  while ((match = pattern.exec(source)) !== null) {
    result += source.slice(cursor, match.index);
    result += String(evaluateExpression(match[0], context, { ...runtime, requireBoolean: false }));
    cursor = pattern.lastIndex;
  }
  if (source.includes("${{") && cursor === 0) fail("template expression is incomplete");
  result += source.slice(cursor);
  return result;
}

async function readRequest() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  const input = Buffer.concat(chunks).toString("utf8");
  const request = JSON.parse(input);
  if (!Array.isArray(request.requests)) fail("requests must be an array");
  return request;
}

async function main() {
  assertNodeVersion();
  const request = await readRequest();
  if (typeof request.package_root !== "string") fail("package_root is required");
  const moduleUrl = pathToFileURL(`${request.package_root}/dist/index.js`).href;
  const api = await import(moduleUrl);
  const results = request.requests.map((item) => {
    if (item.mode === "expression") {
      return {
        value: evaluateExpression(item.source, item.context, {
          api,
          cancelled: item.cancelled,
        }),
      };
    }
    if (item.mode === "template") {
      return {
        value: evaluateTemplate(item.source, item.context, {
          api,
          cancelled: item.cancelled,
        }),
      };
    }
    fail(`unknown request mode: ${item.mode}`);
  });
  process.stdout.write(`${JSON.stringify({ results })}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
