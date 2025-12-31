import { defineConfig } from 'tsdown';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm', 'cjs'],
  clean: true,
  sourcemap: false,
  minify: true,
  outDir: 'dist',
  external: [],
});
