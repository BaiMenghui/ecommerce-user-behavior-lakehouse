import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// 开发服务器：5173 端口，/api 全部代理到 SpringBoot 数据服务
// 说明：本机 Windows 侧 8080 被其他进程占用（PID 非本服务），
// WSL2 的 localhost 转发实际落到宿主 8080 导致 404，
// 故代理目标改用 WSL eth0 IP（重启后 IP 可能变化，可用 wsl hostname -I 重新获取）。
// 若后续 localhost:8080 可直达 SpringBoot，可改回 http://localhost:8080
export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    host: true,
    proxy: {
      '/api': {
        target: 'http://172.22.220.48:8080',
        changeOrigin: true
      }
    }
  }
})
