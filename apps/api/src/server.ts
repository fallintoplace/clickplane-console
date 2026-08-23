import { buildApp } from "./app.js";

const port = Number(process.env.PORT ?? 3001);
const app = buildApp();

await app.listen({ port, host: "0.0.0.0" });
console.log(`ClickPlane API listening on http://localhost:${port}`);
