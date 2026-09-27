import { createApp, defineComponent, h, ref } from "vue";
import {
  createBrahmaputra,
  useConnectionState,
  useLatestByKey,
  usePublish,
  useRecentRecords,
} from "@brahmaputra/ws-vue";
import { config, price, reconnect } from "./common";

const cfg = config();

const Board = defineComponent({
  setup() {
    const state = useConnectionState();
    const { data: prices, error } = useLatestByKey(cfg.prices);
    const { data: tape } = useRecentRecords(cfg.prices, { limit: 5 });
    const publish = usePublish();
    const lastOrder = ref("");
    const orderError = ref("");
    const buy = (symbol: string) =>
      publish({ topic: cfg.orders, value: JSON.stringify({ symbol, qty: 1 }) }).then(
        (ack) => (lastOrder.value = `${ack.partition}:${ack.offset}`),
        (e) => (orderError.value = e.code ?? String(e)),
      );
    return () => {
      const symbols = [...new Set([...cfg.symbols, ...prices.value.keys()])].sort();
      return h("main", [
        h("h1", "Vue"),
        h("p", { id: "state" }, state.value),
        h("p", { id: "feed-error" }, error.value?.code ?? ""),
        h("table", [
          h(
            "tbody",
            symbols.map((s) =>
              h("tr", { key: s, "data-symbol": s }, [
                h("td", s),
                h("td", { class: "price" }, price(prices.value.get(s))),
                h("td", [h("button", { "data-buy": s, onClick: () => buy(s) }, "Buy")]),
              ]),
            ),
          ),
        ]),
        h(
          "ol",
          { id: "tape" },
          tape.value.map((r) => h("li", { key: `${r.partition}:${r.offset}` }, `${r.key} ${price(r)}`)),
        ),
        h("p", { id: "last-order" }, lastOrder.value),
        h("p", { id: "order-error" }, orderError.value),
      ]);
    };
  },
});

createApp(Board)
  .use(createBrahmaputra({ url: cfg.gw, token: cfg.token, reconnect }))
  .mount("#root");
