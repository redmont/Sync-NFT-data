const providerUrl = process.env.ALCHEMY_URL;
const apiKey = process.env.RESERVOIR_API_KEY;
const osKey = process.env.OS_KEY;
let uri = process.env.mongoURL || 'mongodb://localhost:27017';

const { addresses } = require('../data/nft_to_collect.json');

import fetch from 'node-fetch';
import { Multicall, ContractCallResults, ContractCallContext } from 'ethereum-multicall';
require('events').EventEmitter.defaultMaxListeners = process.env.defaultMaxListeners || 10000;
const { MongoClient } = require('mongodb');
const mongoClient = new MongoClient(uri);

const db = {
  NFT: mongoClient.db('BOT_NFT').collection('NFT2'),
  TRAITS: mongoClient.db('BOT_NFT').collection('NFT_DATA2'),

  ERROR_MODE: !!process.env.ERROR_MODE || false,
  DATA_MODE: process.env.DATA_MODE, // ipfs, http or ''

  BATCH_SIZE: Number(process.env.BATCH_SIZE) || 200,
  BATCH_SAVE_SIZE: Number(process.env.BATCH_SAVE_SIZE) || 3000,
  WAIT_TIME: Number(process.env.WAIT_TIME) || 300,

  FETCH_COLLECTIONS: process.env.FETCH_COLLECTIONS,
  FETCH_TOKENS: process.env.FETCH_TOKENS,
  FETCH_METADATA_URL: process.env.FETCH_METADATA_URL,
  FETCH_METADATA: process.env.FETCH_METADATA,
};

const multicall = new Multicall({ nodeUrl: providerUrl, tryAggregate: true });

const contractABI = {
  erc721: [
    {
      name: 'tokenURI',
      type: 'function',
      stateMutability: 'view',
      inputs: [
        {
          type: 'uint256',
          name: 'tokenId',
        },
      ],
      outputs: [{ internalType: 'string', name: '', type: 'string' }],
    },
    { inputs: [{ internalType: 'bytes4', name: 'interfaceId', type: 'bytes4' }], name: 'supportsInterface', outputs: [{ internalType: 'bool', name: '', type: 'bool' }], stateMutability: 'view', type: 'function' },
  ],
  erc1155: [
    {
      name: 'uri',
      type: 'function',
      stateMutability: 'view',
      inputs: [
        {
          type: 'uint256',
          name: 'tokenId',
        },
      ],
      outputs: [{ internalType: 'string', name: '', type: 'string' }],
    },
  ],
};

const options = { method: 'GET', headers: { accept: '*/*', 'x-api-key': apiKey }, timeout: 30000 };

const monitorMemoryUsage = () => {
  setInterval(() => {
    const used = process.memoryUsage();
    for (let key in used) {
      console.log(`${key} ${Math.round((used[key] / 1024 / 1024) * 100) / 100} MB`);
    }
  }, 5 * 60000);
};

const updateDB = async (bulkOps) => {
  await db.NFT.bulkWrite(bulkOps, { ordered: true });
};

const getContractTokens = async (contract, total) => {
  let tokensArray = [];
  let continuation = null;
  let count = 0;

  const fetchURL = async (url, options) => {
    try {
      const response = await fetch(url, options);
      const data = await response.json();
      return data;
    } catch (error) {
      console.log(error);
      await new Promise((resolve) => setTimeout(resolve, 2 * 60000));
      return fetchURL(url, options);
    }
  };

  while (true) {
    const url = `https://api.reservoir.tools/tokens/ids/v1?limit=10000&contract=${contract}${continuation ? `&continuation=${continuation}` : ''}`;

    const data = await fetchURL(url, options);

    tokensArray = tokensArray.concat(data.tokens);
    count += tokensArray.length;
    if (!data.continuation) {
      break;
    } else {
      process.stdout.write(`\r contract= ${contract}, total=${total}, tokensCount: ${count}, continue:${continuation}`);
      continuation = data.continuation;

      if (tokensArray.length > 1000000) {
        const bulkOps = tokensArray.map((token) => ({
          updateOne: {
            filter: { addr_tkn: contract, id_tkn: token },
            update: { $set: {} },
            upsert: true,
          },
        }));

        console.log(`\npre-inserting ${bulkOps.length} tokens to DB\n`);
        await db.TRAITS.bulkWrite(bulkOps, { ordered: true });
        tokensArray = [];
      }
    }
  }

  return tokensArray;
};

let batchSizeMultiCall;
const getMetaDataForContract = async (contractAddr: string, tokens: any[], isERC721: any): Promise<any> => {
  let results: any = {};
  const interfaceABI = isERC721 == true ? contractABI.erc721 : contractABI.erc1155;
  const methodName: string = isERC721 == true ? `tokenURI` : `uri`;

  for (let i = 0; i < tokens.length; i += batchSizeMultiCall) {
    const tokenBatch = tokens.slice(i, i + batchSizeMultiCall);

    try {
      const contractCallContext: ContractCallContext[] = tokenBatch.map((token) => ({
        reference: `${token}`,
        contractAddress: contractAddr,
        abi: interfaceABI,
        calls: [{ reference: 'fooCall', methodName: methodName, methodParameters: [token] }],
      }));

      const batchResults: ContractCallResults = await multicall.call(contractCallContext);
      Object.assign(results, batchResults.results);
      process.stdout.write(`\r Getting metadata url for collection: ${contractAddr}, tokens count: ${tokens.length}, processed: ${Object.keys(results).length}`);
    } catch (error) {
      batchSizeMultiCall = Math.floor(batchSizeMultiCall / 2);
      if (batchSizeMultiCall < 5) {
        return false;
      }
      i = 0;
    }
  }

  return results;
};

const fetchDataFromOS = async (collection, id, retry = 0) => {
  if (retry > 2) {
    return false;
  }
  try {
    const url = `https://api.opensea.io/v2/chain/ethereum/contract/${collection}/nfts/${id}`;
    const options = {
      method: 'GET',
      headers: { accept: 'application/json', 'X-API-KEY': osKey },
    };
    const resp = await fetch(url, options);
    const nft = await resp.json();
    return nft.nft.traits;
  } catch (error) {
    await new Promise((resolve) => setTimeout(resolve, 2000));
    return await fetchDataFromOS(collection, id, retry + 1);
  }
};

const getMetaDataForAllTokens = async (tokens) => {
  const MAX_RETRIES = 2;
  const WAIT_TIME = db.WAIT_TIME;
  const BATCH_SIZE = db.BATCH_SIZE;

  const formatMetadata = (metadata) => {
    if (!metadata.attributes || typeof metadata.attributes !== 'object') return [];

    return Array.isArray(metadata.attributes) ? metadata.attributes.map((attr) => (attr.trait_type && attr.value ? { trait_key: attr.trait_type, trait_value: attr.value } : attr)) : Object.keys(metadata.attributes).map((key) => ({ trait_key: key, trait_value: metadata.attributes[key] }));
  };

  const fetchMetadata = async (token, options, retry = 0) => {
    try {
      let metadata;
      let metadataUrl = token.metaDataURL;

      if (metadataUrl.startsWith('data:application/json;base64,')) {
        metadata = JSON.parse(Buffer.from(metadataUrl.split(',')[1], 'base64').toString());
      } else if (metadataUrl.startsWith('data:application/json')) {
        metadata = JSON.parse(metadataUrl.slice(metadataUrl.indexOf('{')));
      } else if (metadataUrl.startsWith('ipfs://')) {
        metadataUrl = metadataUrl.replace('ipfs://', 'https://ipfs.io/ipfs/');
        const metadataResponse = await fetch(metadataUrl, options);
        metadata = await metadataResponse.json();
      } else if (metadataUrl.startsWith('ar://')) {
        metadataUrl = metadataUrl.replace('ar://', 'https://arweave.net/');
        const metadataResponse = await fetch(metadataUrl, options);
        metadata = await metadataResponse.json();
      } else if (metadataUrl.startsWith('http')) {
        const metadataResponse = await fetch(metadataUrl, options);
        metadata = await metadataResponse.json();
      } else {
        try {
          const traits = await fetchDataFromOS(token.addr_tkn, token.id_tkn);
          if (traits === false) {
            throw 'os traits error';
          }
          return { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, metadata: formatMetadata({ attributes: traits }), error: false };
        } catch (error) {
          console.log(error);
          return { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, error: true, url: token.metaDataURL };
        }
      }

      return { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, metadata: formatMetadata(metadata), error: false };
    } catch (error) {
      try {
        const traits = await fetchDataFromOS(token.addr_tkn, token.id_tkn);
        if (traits === false) {
          throw 'os traits error';
        }
        return { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, metadata: formatMetadata({ attributes: traits }), error: false };
      } catch (error) {
        console.log(error);
        return { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, error: true, url: token.metaDataURL };
      }
    }
  };
  const options = { timeout: 8000 };
  let maxTokensPerBatch = BATCH_SIZE;

  let results = [];
  let errCount = 0;
  let successCount = 0;

  while (tokens.length > 0) {
    const tokensBackup = [...tokens];
    const batch = tokens.splice(0, maxTokensPerBatch);
    const batchResults = await Promise.all(batch.map((token) => fetchMetadata(token, options)));

    results = [...results, ...batchResults];

    const errResults = batchResults.filter((result) => result.error);
    errCount += errResults.length;
    successCount += batchResults.length - errResults.length;

    process.stdout.write(`\r fetchedTotal=${results.length}, fetchedSuccess=${successCount} maxTokensPerBatch=${maxTokensPerBatch} errCount=${errCount}; errorURL=${errResults[0]?.url || ''} `);

    await new Promise((resolve) => setTimeout(resolve, WAIT_TIME));

    if (errCount > 10 && maxTokensPerBatch > 100) {
      maxTokensPerBatch = Math.floor(maxTokensPerBatch / 2);
      tokens = tokensBackup;
    }
  }

  return results;
};

const getCollectionInfo = async (collectionAddr) => {
  const maxInfoPerBatch = 30;
  let results = [];
  while (collectionAddr.length > 0) {
    const batch = collectionAddr.splice(0, maxInfoPerBatch);
    const batchResults = await Promise.all(
      batch.map(async (collectionAddr) => {
        let url = `https://api.reservoir.tools/collections/v6?id=${collectionAddr}`;
        const response = await fetch(url, options);
        const data: any = await response.json();
        const result = data?.collections[0];
        return { addr_tkn: collectionAddr, contractKind: result.contractKind, tokenCount: result.tokenCount };
      })
    );
    results = results.concat(batchResults);
    process.stdout.write(`\r getCollectionInfo: ${results.length}\n`);
    await new Promise((resolve) => setTimeout(resolve, 2100));
  }
  return results;
};

const getTokens = async (nftCollections) => {
  let bulkOps = [];
  const MAX_BULK_OPS_SIZE = 30000;

  const generateBulkOps = (tokens, collection) => {
    return tokens.map((token) => ({
      updateOne: {
        filter: { addr_tkn: collection, id_tkn: token },
        update: { $set: {} },
        upsert: true,
      },
    }));
  };

  const writeBulkOpsToDB = async (bulkOps) => {
    console.log(`Inserting ${bulkOps.length} tokens to DB`);
    await db.TRAITS.bulkWrite(bulkOps, { ordered: true });
  };

  for (let i = 0; i < nftCollections.length; i++) {
    const collection = nftCollections[i];
    const collectionInfo = await db.NFT.findOne({ addr_tkn: collection });
    const total = collectionInfo?.tokenCount;
    const totalInNftData = await db.TRAITS.countDocuments({ addr_tkn: collection });

    if (totalInNftData >= total) {
      process.stdout.write(`\r Contract= ${collection}, total=${total}, totalInNftData: ${totalInNftData}, skipping...\n`);
      continue;
    }

    const startTime = new Date();
    const tokens = await getContractTokens(collection, total);
    const endTime = new Date();
    const timeDifference = endTime.getTime() - startTime.getTime();

    process.stdout.write(`\r Contract= ${collection}, total=${total}, tokensCount: ${tokens.length}, timeDifference: ${timeDifference}ms\n`);

    if (tokens.length === 0) continue;

    bulkOps = bulkOps.concat(generateBulkOps(tokens, collection));

    if (bulkOps.length > MAX_BULK_OPS_SIZE || i === nftCollections.length - 1) {
      await writeBulkOpsToDB(bulkOps);
      bulkOps = [];
    }
  }
};

const getMetadataURL = async (nftCollections) => {
  let bulkOps = [];
  const BATCH_SIZE = 10000;

  const getMetadataForBatch = async (addr_tkn, tokens, isERC721) => {
    const results = await getMetaDataForContract(addr_tkn, tokens, isERC721);
    return tokens.map((token) => {
      const url = results[token]?.callsReturnContext[0]?.returnValues[0];
      let update;
      if (url) {
        update = { $set: { metaDataURL: url } };
      } else {
        update = { $set: { collection_problem: true } };
      }
      return {
        updateOne: {
          filter: { addr_tkn, id_tkn: token },
          update: update,
          upsert: false,
        },
      };
    });
  };

  for (const collectionId of nftCollections) {
    try {
      let collection = await db.NFT.findOne({ addr_tkn: collectionId });
      const totalInNftData = await db.TRAITS.countDocuments({
        addr_tkn: collection.addr_tkn,
        metaDataURL: { $exists: true },
      });

      if (totalInNftData >= collection.tokenCount) {
        process.stdout.write(`\n\n Skipping collection: ${collection.addr_tkn}, tokens count: ${collection.tokenCount}, totalInNftData: ${totalInNftData}`);
        continue;
      }

      process.stdout.write(`\n\n Processing collection: ${collection.addr_tkn}, tokens count: ${collection.tokenCount}`);

      let isERC721 = collection.contractKind === 'erc721' ? true : collection.contractKind === 'erc1155' ? false : null;
      if (isERC721 === null) continue;

      const startTime = new Date();
      let count = 0;
      const query = { addr_tkn: collection.addr_tkn, metaDataURL: { $exists: false } };
      const total = await db.TRAITS.countDocuments(query);

      while (true) {
        const tokens = await db.TRAITS.find(query).limit(BATCH_SIZE).toArray();
        if (tokens.length === 0) break;

        const tokenIds = tokens.map((doc) => doc.id_tkn);
        bulkOps = await getMetadataForBatch(collection.addr_tkn, tokenIds, isERC721);
        if (!bulkOps || bulkOps.length === 0) break;

        await db.TRAITS.bulkWrite(bulkOps, { ordered: true });

        const endTime = new Date();
        const timeDifference = endTime.getTime() - startTime.getTime();
        count += tokenIds.length;

        process.stdout.write(`\r Progress for ${collection.addr_tkn}: ${Math.round((count * 100) / total)}%`);
      }
    } catch (error) {
      console.log(`Error for ${collectionId}`, error);
    }
  }
};

const getMetadataFromURL = async () => {
  const BATCH_SIZE = db.BATCH_SAVE_SIZE;

  const buildQuery = (dataMode: string, errorMode: boolean): any => {
    const query: any = { $and: [] };

    if (dataMode) {
      query.$and.push({
        metaDataURL: { $regex: dataMode, $options: 'i' },
      });
    }

    if (errorMode) {
      query.$and.push({ error: errorMode });
    } else {
      query.$and.push({ error: { $exists: false } });
    }

    return query;
  };

  const processBatch = async (cursor, total: number, dataMode: string) => {
    const tokens = [];
    let count = 0;

    while ((await cursor.hasNext()) && count < BATCH_SIZE) {
      const token = await cursor.next();
      tokens.push(token);
      count++;
    }

    if (tokens.length === 0) return null;

    const results = await getMetaDataForAllTokens(tokens);

    const bulkOps = results.map((result, i) => ({
      updateOne: {
        filter: { addr_tkn: result.addr_tkn, id_tkn: result.id_tkn },
        update: { $set: { traits: result.error ? null : results[i].metadata, error: result.error } },
        upsert: false,
      },
    }));

    console.log('inserting...\n');
    if (bulkOps.length > 0) await db.TRAITS.bulkWrite(bulkOps, { ordered: true });

    return total - BATCH_SIZE;
  };

  try {
    const dataMode = db.DATA_MODE;
    const errorMode = db.ERROR_MODE;
    const query = buildQuery(dataMode, errorMode);

    console.log('query', JSON.stringify(query, null, 2), '\n');

    let total = await db.TRAITS.countDocuments(query);
    const skip = Number(process.env.SKIP) || 0;

    const cursor = db.TRAITS.find(query).skip(skip).batchSize(BATCH_SIZE);

    while (true) {
      process.stdout.write(`\n dataMode: ${dataMode} Total Remaining: ${total}\n\n`);
      total = await processBatch(cursor, total, dataMode);
      if (total === null) break;
    }
  } catch (error) {
    console.error('An error occurred:', error);
  }
};

const fetchCollectionInfo = async () => {
  const results = await getCollectionInfo(addresses);
  const bulkOps = results.map((result) => ({
    updateOne: {
      filter: { addr_tkn: result.addr_tkn },
      update: { $set: result },
      upsert: true,
    },
  }));
  await updateDB(bulkOps);
};

async function executeAndMeasureTime(fn, fnArgs, label) {
  const startTime = new Date();
  await fn(...fnArgs);
  const endTime = new Date();
  const timeDifference = endTime.getTime() - startTime.getTime();
  console.log(`${label}: `, timeDifference);
}

(async () => {
  console.log(`BATCH_SIZE=${db.BATCH_SIZE}, BATCH_SAVE_SIZE=${db.BATCH_SAVE_SIZE}, WAIT_TIME=${db.WAIT_TIME}, ERROR_MODE=${db.ERROR_MODE}, DATA_MODE=${db.DATA_MODE}`);
  monitorMemoryUsage();

  if (db.FETCH_COLLECTIONS) {
    await fetchCollectionInfo();
  }

  if (db.FETCH_TOKENS) {
    await executeAndMeasureTime(getTokens, [addresses], 'fetchTokens');
  }

  if (db.FETCH_METADATA_URL) {
    await executeAndMeasureTime(getMetadataURL, [addresses], 'fetchMetadataURL');
  }

  if (db.FETCH_METADATA) {
    await executeAndMeasureTime(getMetadataFromURL, [], 'fetchMetadata');
  }
})();
