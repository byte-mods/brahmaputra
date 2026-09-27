// What every demo app reads from its URL, and how it formats a price, so
// the browser test can drive all of them identically.
export interface DemoConfig {
  gw: string;
  token: string;
  prices: string;
  orders: string;
  symbols: string[];
}

export function config(): DemoConfig {
  const q = new URLSearchParams(location.search);
  return {
    gw: q.get("gw")!,
    token: q.get("token")!,
    prices: q.get("prices")!,
    orders: q.get("orders")!,
    symbols: (q.get("symbols") ?? "AAPL,MSFT").split(","),
  };
}

export function price(record: { json(): unknown } | undefined): string {
  if (!record) return "-";
  return (record.json() as { price: number }).price.toFixed(2);
}

export const reconnect = { minDelayMs: 50, maxDelayMs: 1000 };
