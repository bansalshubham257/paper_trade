import { defineRailway, github, project, service, postgres } from "railway/iac";

export default defineRailway(() => {
  const db = postgres("postgres");

  const feed = service("feed", {
    source: github("bansalshubham257/paper_trade"),
    start: "python upstox_feed.py",
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  const worker = service("worker", {
    source: github("bansalshubham257/paper_trade"),
    start: "python worker.py",
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  const paperTrader = service("paper-trader", {
    source: github("bansalshubham257/paper_trade"),
    start: "python paper_trader.py",
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  const token = service("token", {
    source: github("bansalshubham257/paper_trade"),
    start: "python generate_token.py",
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  return project("paper-trade", {
    resources: [db, feed, worker, paperTrader, token],
  });
});
