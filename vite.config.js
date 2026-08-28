import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { copyFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

export default defineConfig({
  base: './',
  plugins: [
    vue(),
    {
      name: 'github-pages-entry',
      closeBundle() {
        const output = fileURLToPath(new URL('./dist', import.meta.url))
        const game = fileURLToPath(new URL('./dist/cube_english.html', import.meta.url))
        copyFileSync(game, `${output}/index.html`)
        copyFileSync(game, `${output}/404.html`)
      },
    },
  ],
  build: {
    rollupOptions: {
      input: {
        cubeEnglish: fileURLToPath(new URL('./cube_english.html', import.meta.url)),
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})