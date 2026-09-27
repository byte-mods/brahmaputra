import { StrictMode, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  BrahmaputraProvider,
  useConnectionState,
  useLatestByKey,
  usePublish,
  useRecentRecords,
} from "@brahmaputra/ws-react";
import { config, price, reconnect } from "./common";

const cfg = config();

function Board() {
  const state = useConnectionState();
  // Every symbol, from the snapshot on: the board.
  const { data: prices, error } = useLatestByKey(cfg.prices);
  const { data: tape } = useRecentRecords(cfg.prices, { limit: 5 });
  const publish = usePublish();
  const [lastOrder, setLastOrder] = useState("");
  const [orderError, setOrderError] = useState("");
  const symbols = [...new Set([...cfg.symbols, ...prices.keys()])].sort();
  const buy = (symbol: string) =>
    publish({ topic: cfg.orders, value: JSON.stringify({ symbol, qty: 1 }) }).then(
      (ack) => setLastOrder(`${ack.partition}:${ack.offset}`),
      (e) => setOrderError(e.code ?? String(e)),
    );
  return (
    <main>
      <h1>React</h1>
      <p id="state">{state}</p>
      <p id="feed-error">{error?.code ?? ""}</p>
      <table>
        <tbody>
          {symbols.map((s) => (
            <tr key={s} data-symbol={s}>
              <td>{s}</td>
              <td className="price">{price(prices.get(s))}</td>
              <td>
                <button data-buy={s} onClick={() => buy(s)}>Buy</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <ol id="tape">
        {tape.map((r) => (
          <li key={`${r.partition}:${r.offset}`}>{`${r.key} ${price(r)}`}</li>
        ))}
      </ol>
      <p id="last-order">{lastOrder}</p>
      <p id="order-error">{orderError}</p>
    </main>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrahmaputraProvider options={{ url: cfg.gw, token: cfg.token, reconnect }}>
      <Board />
    </BrahmaputraProvider>
  </StrictMode>,
);
