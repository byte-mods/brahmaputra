// Angular, standalone and zoneless; compiled just in time in the browser so
// the demo needs no Angular CLI (a real app would use AOT; the library is
// the same either way).
import "@angular/compiler";
import { Component, provideZonelessChangeDetection, signal } from "@angular/core";
import { bootstrapApplication } from "@angular/platform-browser";
import {
  injectConnectionState,
  injectLatestByKey,
  injectPublish,
  injectRecentRecords,
  provideBrahmaputra,
} from "@brahmaputra/ws-angular";
import { config, price, reconnect } from "./common";

const cfg = config();

@Component({
  selector: "app-root",
  template: `
    <main>
      <h1>Angular</h1>
      <p id="state">{{ state() }}</p>
      <p id="feed-error">{{ prices.error()?.code ?? "" }}</p>
      <table>
        <tbody>
          @for (s of symbols(); track s) {
            <tr [attr.data-symbol]="s">
              <td>{{ s }}</td>
              <td class="price">{{ priceOf(s) }}</td>
              <td><button [attr.data-buy]="s" (click)="buy(s)">Buy</button></td>
            </tr>
          }
        </tbody>
      </table>
      <ol id="tape">
        @for (r of tape.data(); track r.partition + ':' + r.offset) {
          <li>{{ r.key }} {{ fmt(r) }}</li>
        }
      </ol>
      <p id="last-order">{{ lastOrder() }}</p>
      <p id="order-error">{{ orderError() }}</p>
    </main>
  `,
})
class Board {
  state = injectConnectionState();
  prices = injectLatestByKey(cfg.prices);
  tape = injectRecentRecords(cfg.prices, { limit: 5 });
  publish = injectPublish();
  lastOrder = signal("");
  orderError = signal("");
  fmt = price;

  symbols(): string[] {
    return [...new Set([...cfg.symbols, ...this.prices.data().keys()])].sort();
  }

  priceOf(s: string): string {
    return price(this.prices.data().get(s));
  }

  buy(symbol: string): void {
    this.publish({ topic: cfg.orders, value: JSON.stringify({ symbol, qty: 1 }) }).then(
      (ack) => this.lastOrder.set(`${ack.partition}:${ack.offset}`),
      (e) => this.orderError.set(e.code ?? String(e)),
    );
  }
}

bootstrapApplication(Board, {
  providers: [
    provideZonelessChangeDetection(),
    provideBrahmaputra({ url: cfg.gw, token: cfg.token, reconnect }),
  ],
}).catch((e) => {
  document.body.textContent = `bootstrap failed: ${e}`;
});
