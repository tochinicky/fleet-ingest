/**
 * ESLint for every TypeScript file in the repo (lint = eslint + `tsc --noEmit`). The recommended rule sets,
 * without type-aware rules: `tsc` already checks types. The slice 0 spike is throwaway code and is linted too, but
 * nothing generated or vendored is. `prefer-const` allows a `let` that closures read before its one assignment: the
 * composition roots use that for forward references (the subscriber's gate, the pipeline behind a metrics callback).
 */
import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["**/node_modules/**", "**/data/**", "**/results/**", "deploy/**"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["**/*.ts", "**/*.js"],
    languageOptions: { globals: { ...globals.node } },
    rules: {
      "@typescript-eslint/no-unused-vars": ["error", { argsIgnorePattern: "^_", varsIgnorePattern: "^_" }],
      "prefer-const": ["error", { ignoreReadBeforeAssign: true }],
    },
  },
);
