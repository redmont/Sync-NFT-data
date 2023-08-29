const fetch = require("node-fetch");
const MongoClient = require("mongodb").MongoClient;
const uri = "mongodb://localhost:27017";
const mongoClient = new MongoClient(uri);
const { addresses } = require("../data/nft_to_collect.json");

const db = {
  TRAITS: mongoClient.db("BOT_NFT").collection("TRAITS"),
  START: performance.now(),
  AMT_ADDR_DONE: 0,
  AMT_CALLS: 0,
  AMT_CALL_ATTEMPTS: 0,
};

const logProgress = () => {
  const ram =
    Math.round((process.memoryUsage().heapUsed / 1024 / 1024) * 100) / 100;
  const time = ((performance.now() - db.START) / 1000).toFixed(2);

  process.stdout.write(
    `\x1b[38;5;12mCALLS:\x1b[0m ${db.AMT_CALLS} / ${db.AMT_CALL_ATTEMPTS} ` +
      `\x1b[38;5;12mDONE:\x1b[0m ${db.AMT_ADDR_DONE} / ${addresses.length} ` +
      `\x1b[38;5;12mTIME:\x1b[0m ${time} s ` +
      `\x1b[38;5;12mRAM:\x1b[0m ${ram} MB `
  );
};

const apiCall = async ({ url, options }) => {
  let res;
  db.AMT_CALL_ATTEMPTS++;
  logProgress();

  await fetch(url, options)
    .then((response) => response.json())
    .then((json) => (res = JSON.parse(JSON.stringify(json))))
    .catch((error) => console.error(error));

  if (!res) return;

  db.AMT_CALLS++;
  logProgress();
  return res;
};

const upsertDb = async (addr, idLinkTraits) => {
  //@todo upsert to TRAITS
};

const getTraits = async (links) => {
  //@todo get from apiCall & return {addr: {id: link, ...}}(if !result, log error)
};

const getLinks = async (addr, ids) => {
  //@todo via multicall get from the contract & return res: { id: link... }
};

const getIds = async (addr) => {
  //@todo get from reservoir & return ids
};

(async () => {
  //@note try to optimize, e.g.: 1st get all ids & attach to addr, then in "parallel" get & attach links to it (or smth else)
  //@note remain core logic here
  for (const addr of addresses) {
    const ids = await getIds(addr);
    const idLink = await getLinks(addr, ids);
    const idLinkTraits = await getTraits(idLink);

    if (!idLinkTraits) continue;

    await upsertDb(addr, idLinkTraits);
    db.AMT_ADDR_DONE++;
  }
})();
