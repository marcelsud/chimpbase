/**
 * Read a JSON response body under the shape the route is documented to return.
 *
 * `Response.json()` is declared as returning `any`, which would let a route's
 * actual payload drift from what a test claims to assert on.
 */
export async function readJsonResponse<TBody = unknown>(response: Response | null | undefined): Promise<TBody> {
  if (!(response !== null && response !== undefined)) {
    throw new Error("expected the route to produce a response");
  }

  return await response.json() as TBody;
}
