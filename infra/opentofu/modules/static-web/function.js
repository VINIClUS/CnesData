function handler(event) {
  const request = event.request;
  const uri = request.uri;
  const excluded = uri === "/api" || uri.startsWith("/api/") ||
    uri === "/assets" || uri.startsWith("/assets/") || uri.includes(".");
  if (!excluded) request.uri = "/index.html";
  return request;
}
