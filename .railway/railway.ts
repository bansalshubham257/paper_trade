import { defineRailway, github, project, service, fn, postgres } from "railway/iac";

export default defineRailway(() => {
  const db = postgres("postgres");

  const feed = service("feed", {
    source: github("bansalshubham257/paper_trade", { branch: "master" }),
    start: "python upstox_feed.py",
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  const worker = service("worker", {
    source: github("bansalshubham257/paper_trade", { branch: "master" }),
    start: "python worker.py",
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  const paperTrader = service("paper-trader", {
    source: github("bansalshubham257/paper_trade", { branch: "master" }),
    start: "python paper_trader.py",
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  const token = fn("token", {
    source: github("bansalshubham257/paper_trade", { branch: "master" }),
    start: "sh -c 'python -m playwright install --with-deps chromium && python generate_token.py'",
    deploy: {
      cronSchedule: "30 1 * * *",
    },
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  const instruments = fn("instruments", {
    source: github("bansalshubham257/paper_trade", { branch: "master" }),
    start: "python fetch_instruments.py",
    deploy: {
      cronSchedule: "0 2 * * *",
    },
    env: {
      DATABASE_URL: db.env.DATABASE_URL,
    },
  });

  return project("paper-trade", {
    resources: [db, feed, worker, paperTrader, token, instruments],
  });
});
