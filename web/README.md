# dex-arbitr 前端

Vue 3 + Vite + Naive UI。Rust 进程提供 `/api/*` 并托管构建产物。

```bash
cd web
npm install
npm run build          # 产出 dist/，cargo run 后打开 http://127.0.0.1:8090/
npm run dev            # 开发：http://127.0.0.1:5173 ，API 代理到 8090
```

`config/default.yaml` 的 `http.web_root` 指向 `web/dist`。改完前端必须 `npm run build`，否则进程仍提供上一份静态页。
