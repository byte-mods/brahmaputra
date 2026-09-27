// Bundle each demo app with esbuild. The SDK packages are taken from their
// source folders' dist/, and every shared dependency (React, Vue, Angular,
// RxJS, the core client) is resolved from this folder, so each app has
// exactly one copy of each.
import { build } from "esbuild";
import { mkdir, writeFile } from "node:fs/promises";

const alias = {
  "@brahmaputra/ws-client": "../js/dist/index.js",
  "@brahmaputra/ws-react": "../react/dist/index.js",
  "@brahmaputra/ws-vue": "../vue/dist/index.js",
  "@brahmaputra/ws-angular": "../angular/dist/index.js",
};

// Shared packages resolve from here wherever they are imported from (the
// SDK folders have dev copies of their own), keeping package exports intact.
const shared = /^(react|react-dom|vue|rxjs|@angular\/[a-z-]+)(\/.*)?$/;
const here = process.cwd();
const dedupe = {
  name: "dedupe",
  setup(b) {
    b.onResolve({ filter: shared }, (args) => {
      if (args.pluginData === "dedupe") return undefined;
      return b.resolve(args.path, { resolveDir: here, kind: args.kind, pluginData: "dedupe" });
    });
  },
};

for (const [name, entry] of [
  ["react", "apps/react.tsx"],
  ["vue", "apps/vue.ts"],
  ["angular", "apps/angular.ts"],
]) {
  await mkdir(`dist/${name}`, { recursive: true });
  await build({
    entryPoints: [entry],
    bundle: true,
    format: "esm",
    target: "es2022",
    outfile: `dist/${name}/app.js`,
    alias,
    plugins: [dedupe],
    jsx: "automatic",
    define: {
      "process.env.NODE_ENV": '"production"',
      __VUE_OPTIONS_API__: "false",
      __VUE_PROD_DEVTOOLS__: "false",
      __VUE_PROD_HYDRATION_MISMATCH_DETAILS__: "false",
    },
    tsconfigRaw: { compilerOptions: { experimentalDecorators: true, useDefineForClassFields: false } },
    logLevel: "warning",
    minify: true,
  });
  const root = name === "angular" ? "<app-root></app-root>" : '<div id="root"></div>';
  await writeFile(
    `dist/${name}/index.html`,
    `<!doctype html><html><head><meta charset="utf-8"><title>${name} trading screen</title></head>` +
      `<body>${root}<script type="module" src="app.js"></script></body></html>`,
  );
}
console.log("built dist/{react,vue,angular}");
