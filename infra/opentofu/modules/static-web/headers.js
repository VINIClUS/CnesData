const HEADERS = ${headers};

function handler(event) {
  const response = event.response;
  Object.keys(HEADERS).forEach(function (name) {
    response.headers[name] = { value: HEADERS[name] };
  });
  return response;
}
