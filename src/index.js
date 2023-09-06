"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __spreadArray = (this && this.__spreadArray) || function (to, from, pack) {
    if (pack || arguments.length === 2) for (var i = 0, l = from.length, ar; i < l; i++) {
        if (ar || !(i in from)) {
            if (!ar) ar = Array.prototype.slice.call(from, 0, i);
            ar[i] = from[i];
        }
    }
    return to.concat(ar || Array.prototype.slice.call(from));
};
Object.defineProperty(exports, "__esModule", { value: true });
var providerUrl = process.env.ALCHEMY_URL;
var apiKey = process.env.RESERVOIR_API_KEY;
var osKey = process.env.OS_KEY;
var uri = process.env.mongoURL || 'mongodb://localhost:27017';
var addresses = require('../data/nft_to_collect.json').addresses;
var node_fetch_1 = require("node-fetch");
var _a = require('@opensea/stream-js'), OpenSeaStreamClient = _a.OpenSeaStreamClient, EventType = _a.EventType, Network = _a.Network;
var WebSocket = require('ws').WebSocket;
var ethereum_multicall_1 = require("ethereum-multicall");
require('events').EventEmitter.defaultMaxListeners = process.env.defaultMaxListeners || 10000;
var MongoClient = require('mongodb').MongoClient;
var mongoClient = new MongoClient(uri);
var db = {
    OS_SUB_EVENTS: [EventType.ITEM_METADATA_UPDATED],
    NFT: mongoClient.db('BOT_NFT').collection('NFT2'),
    TRAITS: mongoClient.db('BOT_NFT').collection('NFT_DATA2'),
    ERROR_MODE: !!process.env.ERROR_MODE || false,
    DATA_MODE: process.env.DATA_MODE,
    BATCH_SIZE: Number(process.env.BATCH_SIZE) || 200,
    BATCH_SAVE_SIZE: Number(process.env.BATCH_SAVE_SIZE) || 3000,
    WAIT_TIME: Number(process.env.WAIT_TIME) || 300,
    FETCH_COLLECTIONS: process.env.FETCH_COLLECTIONS,
    FETCH_TOKENS: process.env.FETCH_TOKENS,
    FETCH_METADATA_URL: process.env.FETCH_METADATA_URL,
    FETCH_METADATA: process.env.FETCH_METADATA,
};
var osClient = new OpenSeaStreamClient({
    token: process.env.OS_KEY,
    networkName: Network.MAINNET,
    connectOptions: {
        transport: WebSocket,
    },
});
var multicall = new ethereum_multicall_1.Multicall({ nodeUrl: providerUrl, tryAggregate: true });
var contractABI = {
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
var options = { method: 'GET', headers: { accept: '*/*', 'x-api-key': apiKey }, timeout: 30000 };
var monitorMemoryUsage = function () {
    setInterval(function () {
        var used = process.memoryUsage();
        for (var key in used) {
            console.log("".concat(key, " ").concat(Math.round((used[key] / 1024 / 1024) * 100) / 100, " MB"));
        }
    }, 5 * 60000);
};
var updateDB = function (bulkOps) { return __awaiter(void 0, void 0, void 0, function () {
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, db.NFT.bulkWrite(bulkOps, { ordered: true })];
            case 1:
                _a.sent();
                return [2 /*return*/];
        }
    });
}); };
var getContractTokens = function (contract, total) { return __awaiter(void 0, void 0, void 0, function () {
    var tokensArray, continuation, count, fetchURL, url, data, bulkOps;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                tokensArray = [];
                continuation = null;
                count = 0;
                fetchURL = function (url, options) { return __awaiter(void 0, void 0, void 0, function () {
                    var response, data, error_1;
                    return __generator(this, function (_a) {
                        switch (_a.label) {
                            case 0:
                                _a.trys.push([0, 3, , 5]);
                                return [4 /*yield*/, (0, node_fetch_1.default)(url, options)];
                            case 1:
                                response = _a.sent();
                                return [4 /*yield*/, response.json()];
                            case 2:
                                data = _a.sent();
                                return [2 /*return*/, data];
                            case 3:
                                error_1 = _a.sent();
                                console.log(error_1);
                                return [4 /*yield*/, new Promise(function (resolve) { return setTimeout(resolve, 2 * 60000); })];
                            case 4:
                                _a.sent();
                                return [2 /*return*/, fetchURL(url, options)];
                            case 5: return [2 /*return*/];
                        }
                    });
                }); };
                _a.label = 1;
            case 1:
                if (!true) return [3 /*break*/, 6];
                url = "https://api.reservoir.tools/tokens/ids/v1?limit=10000&contract=".concat(contract).concat(continuation ? "&continuation=".concat(continuation) : '');
                return [4 /*yield*/, fetchURL(url, options)];
            case 2:
                data = _a.sent();
                tokensArray = tokensArray.concat(data.tokens);
                count += tokensArray.length;
                if (!!data.continuation) return [3 /*break*/, 3];
                return [3 /*break*/, 6];
            case 3:
                process.stdout.write("\r contract= ".concat(contract, ", total=").concat(total, ", tokensCount: ").concat(count, ", continue:").concat(continuation));
                continuation = data.continuation;
                if (!(tokensArray.length > 1000000)) return [3 /*break*/, 5];
                bulkOps = tokensArray.map(function (token) { return ({
                    updateOne: {
                        filter: { addr_tkn: contract, id_tkn: token },
                        update: { $set: {} },
                        upsert: true,
                    },
                }); });
                console.log("\npre-inserting ".concat(bulkOps.length, " tokens to DB\n"));
                return [4 /*yield*/, db.TRAITS.bulkWrite(bulkOps, { ordered: true })];
            case 4:
                _a.sent();
                tokensArray = [];
                _a.label = 5;
            case 5: return [3 /*break*/, 1];
            case 6: return [2 /*return*/, tokensArray];
        }
    });
}); };
var batchSizeMultiCall;
var getMetaDataForContract = function (contractAddr, tokens, isERC721) { return __awaiter(void 0, void 0, void 0, function () {
    var results, interfaceABI, methodName, i, tokenBatch, contractCallContext, batchResults, error_2;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                results = {};
                interfaceABI = isERC721 == true ? contractABI.erc721 : contractABI.erc1155;
                methodName = isERC721 == true ? "tokenURI" : "uri";
                i = 0;
                _a.label = 1;
            case 1:
                if (!(i < tokens.length)) return [3 /*break*/, 6];
                tokenBatch = tokens.slice(i, i + batchSizeMultiCall);
                _a.label = 2;
            case 2:
                _a.trys.push([2, 4, , 5]);
                contractCallContext = tokenBatch.map(function (token) { return ({
                    reference: "".concat(token),
                    contractAddress: contractAddr,
                    abi: interfaceABI,
                    calls: [{ reference: 'fooCall', methodName: methodName, methodParameters: [token] }],
                }); });
                return [4 /*yield*/, multicall.call(contractCallContext)];
            case 3:
                batchResults = _a.sent();
                Object.assign(results, batchResults.results);
                process.stdout.write("\r Getting metadata url for collection: ".concat(contractAddr, ", tokens count: ").concat(tokens.length, ", processed: ").concat(Object.keys(results).length));
                return [3 /*break*/, 5];
            case 4:
                error_2 = _a.sent();
                batchSizeMultiCall = Math.floor(batchSizeMultiCall / 2);
                if (batchSizeMultiCall < 5) {
                    return [2 /*return*/, false];
                }
                i = 0;
                return [3 /*break*/, 5];
            case 5:
                i += batchSizeMultiCall;
                return [3 /*break*/, 1];
            case 6: return [2 /*return*/, results];
        }
    });
}); };
var fetchDataFromOS = function (collection, id, retry) {
    if (retry === void 0) { retry = 0; }
    return __awaiter(void 0, void 0, void 0, function () {
        var url, options_1, resp, nft, error_3;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    if (retry > 2) {
                        return [2 /*return*/, false];
                    }
                    _a.label = 1;
                case 1:
                    _a.trys.push([1, 4, , 7]);
                    url = "https://api.opensea.io/v2/chain/ethereum/contract/".concat(collection, "/nfts/").concat(id);
                    options_1 = {
                        method: 'GET',
                        headers: { accept: 'application/json', 'X-API-KEY': osKey },
                    };
                    return [4 /*yield*/, (0, node_fetch_1.default)(url, options_1)];
                case 2:
                    resp = _a.sent();
                    return [4 /*yield*/, resp.json()];
                case 3:
                    nft = _a.sent();
                    return [2 /*return*/, nft.nft.traits];
                case 4:
                    error_3 = _a.sent();
                    return [4 /*yield*/, new Promise(function (resolve) { return setTimeout(resolve, 2000); })];
                case 5:
                    _a.sent();
                    return [4 /*yield*/, fetchDataFromOS(collection, id, retry + 1)];
                case 6: return [2 /*return*/, _a.sent()];
                case 7: return [2 /*return*/];
            }
        });
    });
};
var formatMetadata = function (metadata) {
    if (Array.isArray(metadata)) {
        return metadata.map(function (attr) { return ({
            trait_key: attr.trait_type,
            trait_value: attr.value,
        }); });
    }
    if ((metadata === null || metadata === void 0 ? void 0 : metadata.attributes) && typeof metadata.attributes === 'object') {
        return Object.keys(metadata.attributes).map(function (key) { return ({
            trait_key: key,
            trait_value: metadata.attributes[key],
        }); });
    }
    return [];
};
var getMetaDataForAllTokens = function (tokens) { return __awaiter(void 0, void 0, void 0, function () {
    var MAX_RETRIES, WAIT_TIME, BATCH_SIZE, fetchMetadata, options, maxTokensPerBatch, results, errCount, successCount, tokensBackup, batch, batchResults, errResults;
    var _a;
    return __generator(this, function (_b) {
        switch (_b.label) {
            case 0:
                MAX_RETRIES = 2;
                WAIT_TIME = db.WAIT_TIME;
                BATCH_SIZE = db.BATCH_SIZE;
                fetchMetadata = function (token, options, retry) {
                    if (retry === void 0) { retry = 0; }
                    return __awaiter(void 0, void 0, void 0, function () {
                        var metadata, metadataUrl, metadataResponse, metadataResponse, metadataResponse, traits, error_4, error_5, traits, error_6;
                        return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0:
                                    _a.trys.push([0, 15, , 20]);
                                    metadata = void 0;
                                    metadataUrl = token.metaDataURL;
                                    if (!metadataUrl.startsWith('data:application/json;base64,')) return [3 /*break*/, 1];
                                    metadata = JSON.parse(Buffer.from(metadataUrl.split(',')[1], 'base64').toString());
                                    return [3 /*break*/, 14];
                                case 1:
                                    if (!metadataUrl.startsWith('data:application/json')) return [3 /*break*/, 2];
                                    metadata = JSON.parse(metadataUrl.slice(metadataUrl.indexOf('{')));
                                    return [3 /*break*/, 14];
                                case 2:
                                    if (!metadataUrl.startsWith('ipfs://')) return [3 /*break*/, 5];
                                    metadataUrl = metadataUrl.replace('ipfs://', 'https://ipfs.io/ipfs/');
                                    return [4 /*yield*/, (0, node_fetch_1.default)(metadataUrl, options)];
                                case 3:
                                    metadataResponse = _a.sent();
                                    return [4 /*yield*/, metadataResponse.json()];
                                case 4:
                                    metadata = _a.sent();
                                    return [3 /*break*/, 14];
                                case 5:
                                    if (!metadataUrl.startsWith('ar://')) return [3 /*break*/, 8];
                                    metadataUrl = metadataUrl.replace('ar://', 'https://arweave.net/');
                                    return [4 /*yield*/, (0, node_fetch_1.default)(metadataUrl, options)];
                                case 6:
                                    metadataResponse = _a.sent();
                                    return [4 /*yield*/, metadataResponse.json()];
                                case 7:
                                    metadata = _a.sent();
                                    return [3 /*break*/, 14];
                                case 8:
                                    if (!metadataUrl.startsWith('http')) return [3 /*break*/, 11];
                                    return [4 /*yield*/, (0, node_fetch_1.default)(metadataUrl, options)];
                                case 9:
                                    metadataResponse = _a.sent();
                                    return [4 /*yield*/, metadataResponse.json()];
                                case 10:
                                    metadata = _a.sent();
                                    return [3 /*break*/, 14];
                                case 11:
                                    _a.trys.push([11, 13, , 14]);
                                    return [4 /*yield*/, fetchDataFromOS(token.addr_tkn, token.id_tkn)];
                                case 12:
                                    traits = _a.sent();
                                    if (traits === false) {
                                        throw 'os traits error';
                                    }
                                    return [2 /*return*/, { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, metadata: formatMetadata({ attributes: traits }), error: false }];
                                case 13:
                                    error_4 = _a.sent();
                                    console.log(error_4);
                                    return [2 /*return*/, { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, error: true, url: token.metaDataURL }];
                                case 14: return [2 /*return*/, { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, metadata: formatMetadata(metadata), error: false }];
                                case 15:
                                    error_5 = _a.sent();
                                    _a.label = 16;
                                case 16:
                                    _a.trys.push([16, 18, , 19]);
                                    return [4 /*yield*/, fetchDataFromOS(token.addr_tkn, token.id_tkn)];
                                case 17:
                                    traits = _a.sent();
                                    if (traits === false) {
                                        throw 'os traits error';
                                    }
                                    return [2 /*return*/, { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, metadata: formatMetadata({ attributes: traits }), error: false }];
                                case 18:
                                    error_6 = _a.sent();
                                    console.log(error_6);
                                    return [2 /*return*/, { addr_tkn: token.addr_tkn, id_tkn: token.id_tkn, error: true, url: token.metaDataURL }];
                                case 19: return [3 /*break*/, 20];
                                case 20: return [2 /*return*/];
                            }
                        });
                    });
                };
                options = { timeout: 8000 };
                maxTokensPerBatch = BATCH_SIZE;
                results = [];
                errCount = 0;
                successCount = 0;
                _b.label = 1;
            case 1:
                if (!(tokens.length > 0)) return [3 /*break*/, 4];
                tokensBackup = __spreadArray([], tokens, true);
                batch = tokens.splice(0, maxTokensPerBatch);
                return [4 /*yield*/, Promise.all(batch.map(function (token) { return fetchMetadata(token, options); }))];
            case 2:
                batchResults = _b.sent();
                results = __spreadArray(__spreadArray([], results, true), batchResults, true);
                errResults = batchResults.filter(function (result) { return result.error; });
                errCount += errResults.length;
                successCount += batchResults.length - errResults.length;
                process.stdout.write("\r fetchedTotal=".concat(results.length, ", fetchedSuccess=").concat(successCount, " maxTokensPerBatch=").concat(maxTokensPerBatch, " errCount=").concat(errCount, "; errorURL=").concat(((_a = errResults[0]) === null || _a === void 0 ? void 0 : _a.url) || '', " "));
                return [4 /*yield*/, new Promise(function (resolve) { return setTimeout(resolve, WAIT_TIME); })];
            case 3:
                _b.sent();
                if (errCount > 10 && maxTokensPerBatch > 100) {
                    maxTokensPerBatch = Math.floor(maxTokensPerBatch / 2);
                    tokens = tokensBackup;
                }
                return [3 /*break*/, 1];
            case 4: return [2 /*return*/, results];
        }
    });
}); };
var getCollectionInfo = function (collectionAddr) { return __awaiter(void 0, void 0, void 0, function () {
    var maxInfoPerBatch, results, batch, batchResults;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                maxInfoPerBatch = 30;
                results = [];
                _a.label = 1;
            case 1:
                if (!(collectionAddr.length > 0)) return [3 /*break*/, 4];
                batch = collectionAddr.splice(0, maxInfoPerBatch);
                return [4 /*yield*/, Promise.all(batch.map(function (collectionAddr) { return __awaiter(void 0, void 0, void 0, function () {
                        var url, response, data, result;
                        return __generator(this, function (_a) {
                            switch (_a.label) {
                                case 0:
                                    url = "https://api.reservoir.tools/collections/v6?id=".concat(collectionAddr);
                                    return [4 /*yield*/, (0, node_fetch_1.default)(url, options)];
                                case 1:
                                    response = _a.sent();
                                    return [4 /*yield*/, response.json()];
                                case 2:
                                    data = _a.sent();
                                    result = data === null || data === void 0 ? void 0 : data.collections[0];
                                    return [2 /*return*/, { addr_tkn: collectionAddr, contractKind: result.contractKind, tokenCount: result.tokenCount }];
                            }
                        });
                    }); }))];
            case 2:
                batchResults = _a.sent();
                results = results.concat(batchResults);
                process.stdout.write("\r getCollectionInfo: ".concat(results.length, "\n"));
                return [4 /*yield*/, new Promise(function (resolve) { return setTimeout(resolve, 2100); })];
            case 3:
                _a.sent();
                return [3 /*break*/, 1];
            case 4: return [2 /*return*/, results];
        }
    });
}); };
var getTokens = function (nftCollections) { return __awaiter(void 0, void 0, void 0, function () {
    var bulkOps, MAX_BULK_OPS_SIZE, generateBulkOps, writeBulkOpsToDB, i, collection, collectionInfo, total, totalInNftData, startTime, tokens, endTime, timeDifference;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                bulkOps = [];
                MAX_BULK_OPS_SIZE = 30000;
                generateBulkOps = function (tokens, collection) {
                    return tokens.map(function (token) { return ({
                        updateOne: {
                            filter: { addr_tkn: collection, id_tkn: token },
                            update: { $set: {} },
                            upsert: true,
                        },
                    }); });
                };
                writeBulkOpsToDB = function (bulkOps) { return __awaiter(void 0, void 0, void 0, function () {
                    return __generator(this, function (_a) {
                        switch (_a.label) {
                            case 0:
                                console.log("Inserting ".concat(bulkOps.length, " tokens to DB"));
                                return [4 /*yield*/, db.TRAITS.bulkWrite(bulkOps, { ordered: true })];
                            case 1:
                                _a.sent();
                                return [2 /*return*/];
                        }
                    });
                }); };
                i = 0;
                _a.label = 1;
            case 1:
                if (!(i < nftCollections.length)) return [3 /*break*/, 7];
                collection = nftCollections[i];
                return [4 /*yield*/, db.NFT.findOne({ addr_tkn: collection })];
            case 2:
                collectionInfo = _a.sent();
                total = collectionInfo === null || collectionInfo === void 0 ? void 0 : collectionInfo.tokenCount;
                return [4 /*yield*/, db.TRAITS.countDocuments({ addr_tkn: collection })];
            case 3:
                totalInNftData = _a.sent();
                if (totalInNftData >= total) {
                    process.stdout.write("\r Contract= ".concat(collection, ", total=").concat(total, ", totalInNftData: ").concat(totalInNftData, ", skipping...\n"));
                    return [3 /*break*/, 6];
                }
                startTime = new Date();
                return [4 /*yield*/, getContractTokens(collection, total)];
            case 4:
                tokens = _a.sent();
                endTime = new Date();
                timeDifference = endTime.getTime() - startTime.getTime();
                process.stdout.write("\r Contract= ".concat(collection, ", total=").concat(total, ", tokensCount: ").concat(tokens.length, ", timeDifference: ").concat(timeDifference, "ms\n"));
                if (tokens.length === 0)
                    return [3 /*break*/, 6];
                bulkOps = bulkOps.concat(generateBulkOps(tokens, collection));
                if (!(bulkOps.length > MAX_BULK_OPS_SIZE || i === nftCollections.length - 1)) return [3 /*break*/, 6];
                return [4 /*yield*/, writeBulkOpsToDB(bulkOps)];
            case 5:
                _a.sent();
                bulkOps = [];
                _a.label = 6;
            case 6:
                i++;
                return [3 /*break*/, 1];
            case 7: return [2 /*return*/];
        }
    });
}); };
var getMetadataURL = function (nftCollections) { return __awaiter(void 0, void 0, void 0, function () {
    var bulkOps, BATCH_SIZE, getMetadataForBatch, _i, nftCollections_1, collectionId, collection, totalInNftData, isERC721, startTime, count, query, total, tokens, tokenIds, endTime, timeDifference, error_7;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                bulkOps = [];
                BATCH_SIZE = 10000;
                getMetadataForBatch = function (addr_tkn, tokens, isERC721) { return __awaiter(void 0, void 0, void 0, function () {
                    var results;
                    return __generator(this, function (_a) {
                        switch (_a.label) {
                            case 0: return [4 /*yield*/, getMetaDataForContract(addr_tkn, tokens, isERC721)];
                            case 1:
                                results = _a.sent();
                                return [2 /*return*/, tokens.map(function (token) {
                                        var _a, _b;
                                        var url = (_b = (_a = results[token]) === null || _a === void 0 ? void 0 : _a.callsReturnContext[0]) === null || _b === void 0 ? void 0 : _b.returnValues[0];
                                        var update;
                                        if (url) {
                                            update = { $set: { metaDataURL: url } };
                                        }
                                        else {
                                            update = { $set: { collection_problem: true } };
                                        }
                                        return {
                                            updateOne: {
                                                filter: { addr_tkn: addr_tkn, id_tkn: token },
                                                update: update,
                                                upsert: false,
                                            },
                                        };
                                    })];
                        }
                    });
                }); };
                _i = 0, nftCollections_1 = nftCollections;
                _a.label = 1;
            case 1:
                if (!(_i < nftCollections_1.length)) return [3 /*break*/, 13];
                collectionId = nftCollections_1[_i];
                _a.label = 2;
            case 2:
                _a.trys.push([2, 11, , 12]);
                return [4 /*yield*/, db.NFT.findOne({ addr_tkn: collectionId })];
            case 3:
                collection = _a.sent();
                return [4 /*yield*/, db.TRAITS.countDocuments({
                        addr_tkn: collection.addr_tkn,
                        metaDataURL: { $exists: true },
                    })];
            case 4:
                totalInNftData = _a.sent();
                if (totalInNftData >= collection.tokenCount) {
                    process.stdout.write("\n\n Skipping collection: ".concat(collection.addr_tkn, ", tokens count: ").concat(collection.tokenCount, ", totalInNftData: ").concat(totalInNftData));
                    return [3 /*break*/, 12];
                }
                process.stdout.write("\n\n Processing collection: ".concat(collection.addr_tkn, ", tokens count: ").concat(collection.tokenCount));
                isERC721 = collection.contractKind === 'erc721' ? true : collection.contractKind === 'erc1155' ? false : null;
                if (isERC721 === null)
                    return [3 /*break*/, 12];
                startTime = new Date();
                count = 0;
                query = { addr_tkn: collection.addr_tkn, metaDataURL: { $exists: false } };
                return [4 /*yield*/, db.TRAITS.countDocuments(query)];
            case 5:
                total = _a.sent();
                _a.label = 6;
            case 6:
                if (!true) return [3 /*break*/, 10];
                return [4 /*yield*/, db.TRAITS.find(query).limit(BATCH_SIZE).toArray()];
            case 7:
                tokens = _a.sent();
                if (tokens.length === 0)
                    return [3 /*break*/, 10];
                tokenIds = tokens.map(function (doc) { return doc.id_tkn; });
                return [4 /*yield*/, getMetadataForBatch(collection.addr_tkn, tokenIds, isERC721)];
            case 8:
                bulkOps = _a.sent();
                if (!bulkOps || bulkOps.length === 0)
                    return [3 /*break*/, 10];
                return [4 /*yield*/, db.TRAITS.bulkWrite(bulkOps, { ordered: true })];
            case 9:
                _a.sent();
                endTime = new Date();
                timeDifference = endTime.getTime() - startTime.getTime();
                count += tokenIds.length;
                process.stdout.write("\r Progress for ".concat(collection.addr_tkn, ": ").concat(Math.round((count * 100) / total), "%"));
                return [3 /*break*/, 6];
            case 10: return [3 /*break*/, 12];
            case 11:
                error_7 = _a.sent();
                console.log("Error for ".concat(collectionId), error_7);
                return [3 /*break*/, 12];
            case 12:
                _i++;
                return [3 /*break*/, 1];
            case 13: return [2 /*return*/];
        }
    });
}); };
var getMetadataFromURL = function () { return __awaiter(void 0, void 0, void 0, function () {
    var BATCH_SIZE, buildQuery, processBatch, dataMode, errorMode, query, total, skip, cursor, error_8;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                BATCH_SIZE = db.BATCH_SAVE_SIZE;
                buildQuery = function (dataMode, errorMode) {
                    var query = { $and: [] };
                    if (dataMode) {
                        query.$and.push({
                            metaDataURL: { $regex: dataMode, $options: 'i' },
                        });
                    }
                    if (errorMode) {
                        query.$and.push({ error: errorMode });
                    }
                    else {
                        query.$and.push({ error: { $exists: false } });
                    }
                    return query;
                };
                processBatch = function (cursor, total, dataMode) { return __awaiter(void 0, void 0, void 0, function () {
                    var tokens, count, token, results, bulkOps;
                    return __generator(this, function (_a) {
                        switch (_a.label) {
                            case 0:
                                tokens = [];
                                count = 0;
                                _a.label = 1;
                            case 1: return [4 /*yield*/, cursor.hasNext()];
                            case 2:
                                if (!((_a.sent()) && count < BATCH_SIZE)) return [3 /*break*/, 4];
                                return [4 /*yield*/, cursor.next()];
                            case 3:
                                token = _a.sent();
                                tokens.push(token);
                                count++;
                                return [3 /*break*/, 1];
                            case 4:
                                if (tokens.length === 0)
                                    return [2 /*return*/, null];
                                return [4 /*yield*/, getMetaDataForAllTokens(tokens)];
                            case 5:
                                results = _a.sent();
                                bulkOps = results.map(function (result, i) { return ({
                                    updateOne: {
                                        filter: { addr_tkn: result.addr_tkn, id_tkn: result.id_tkn },
                                        update: { $set: { traits: result.error ? null : results[i].metadata, error: result.error } },
                                        upsert: false,
                                    },
                                }); });
                                console.log('inserting...\n');
                                if (!(bulkOps.length > 0)) return [3 /*break*/, 7];
                                return [4 /*yield*/, db.TRAITS.bulkWrite(bulkOps, { ordered: true })];
                            case 6:
                                _a.sent();
                                _a.label = 7;
                            case 7: return [2 /*return*/, total - BATCH_SIZE];
                        }
                    });
                }); };
                _a.label = 1;
            case 1:
                _a.trys.push([1, 6, , 7]);
                dataMode = db.DATA_MODE;
                errorMode = db.ERROR_MODE;
                query = buildQuery(dataMode, errorMode);
                console.log('query', JSON.stringify(query, null, 2), '\n');
                return [4 /*yield*/, db.TRAITS.countDocuments(query)];
            case 2:
                total = _a.sent();
                skip = Number(process.env.SKIP) || 0;
                cursor = db.TRAITS.find(query).skip(skip).batchSize(BATCH_SIZE);
                _a.label = 3;
            case 3:
                if (!true) return [3 /*break*/, 5];
                process.stdout.write("\n dataMode: ".concat(dataMode, " Total Remaining: ").concat(total, "\n\n"));
                return [4 /*yield*/, processBatch(cursor, total, dataMode)];
            case 4:
                total = _a.sent();
                if (total === null)
                    return [3 /*break*/, 5];
                return [3 /*break*/, 3];
            case 5: return [3 /*break*/, 7];
            case 6:
                error_8 = _a.sent();
                console.error('An error occurred:', error_8);
                return [3 /*break*/, 7];
            case 7: return [2 /*return*/];
        }
    });
}); };
var fetchCollectionInfo = function () { return __awaiter(void 0, void 0, void 0, function () {
    var results, bulkOps;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0: return [4 /*yield*/, getCollectionInfo(addresses)];
            case 1:
                results = _a.sent();
                bulkOps = results.map(function (result) { return ({
                    updateOne: {
                        filter: { addr_tkn: result.addr_tkn },
                        update: { $set: result },
                        upsert: true,
                    },
                }); });
                return [4 /*yield*/, updateDB(bulkOps)];
            case 2:
                _a.sent();
                return [2 /*return*/];
        }
    });
}); };
function executeAndMeasureTime(fn, fnArgs, label) {
    return __awaiter(this, void 0, void 0, function () {
        var startTime, endTime, timeDifference;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    startTime = new Date();
                    return [4 /*yield*/, fn.apply(void 0, fnArgs)];
                case 1:
                    _a.sent();
                    endTime = new Date();
                    timeDifference = endTime.getTime() - startTime.getTime();
                    console.log("".concat(label, ": "), timeDifference);
                    return [2 /*return*/];
            }
        });
    });
}
var subMetaDataUpdate = function () { return __awaiter(void 0, void 0, void 0, function () {
    return __generator(this, function (_a) {
        osClient.onEvents('*', db.OS_SUB_EVENTS, function (event) { return __awaiter(void 0, void 0, void 0, function () {
            var metadata, nftId, info, _a, addr_tkn, id_tkn, traits, e_1;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        _b.trys.push([0, 3, , 4]);
                        metadata = event.payload.item.metadata;
                        nftId = event.payload.item.nft_id;
                        info = nftId.slice(nftId.indexOf('/') + 1);
                        _a = info.split('/'), addr_tkn = _a[0], id_tkn = _a[1];
                        traits = formatMetadata(metadata.traits);
                        if (!(traits.length > 0)) return [3 /*break*/, 2];
                        return [4 /*yield*/, db.TRAITS.updateOne({ addr_tkn: addr_tkn, id_tkn: id_tkn }, {
                                $set: {
                                    traits: traits,
                                    metaDataURL: metadata.metadata_url,
                                    error: false,
                                    updatedAt: new Date(),
                                },
                            }, { upsert: true })];
                    case 1:
                        _b.sent();
                        _b.label = 2;
                    case 2: return [3 /*break*/, 4];
                    case 3:
                        e_1 = _b.sent();
                        return [3 /*break*/, 4];
                    case 4: return [2 /*return*/];
                }
            });
        }); });
        return [2 /*return*/];
    });
}); };
(function () { return __awaiter(void 0, void 0, void 0, function () {
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                console.log("BATCH_SIZE=".concat(db.BATCH_SIZE, ", BATCH_SAVE_SIZE=").concat(db.BATCH_SAVE_SIZE, ", WAIT_TIME=").concat(db.WAIT_TIME, ", ERROR_MODE=").concat(db.ERROR_MODE, ", DATA_MODE=").concat(db.DATA_MODE));
                monitorMemoryUsage();
                subMetaDataUpdate();
                if (!db.FETCH_COLLECTIONS) return [3 /*break*/, 2];
                return [4 /*yield*/, fetchCollectionInfo()];
            case 1:
                _a.sent();
                _a.label = 2;
            case 2:
                if (!db.FETCH_TOKENS) return [3 /*break*/, 4];
                return [4 /*yield*/, executeAndMeasureTime(getTokens, [addresses], 'fetchTokens')];
            case 3:
                _a.sent();
                _a.label = 4;
            case 4:
                if (!db.FETCH_METADATA_URL) return [3 /*break*/, 6];
                return [4 /*yield*/, executeAndMeasureTime(getMetadataURL, [addresses], 'fetchMetadataURL')];
            case 5:
                _a.sent();
                _a.label = 6;
            case 6:
                if (!db.FETCH_METADATA) return [3 /*break*/, 8];
                return [4 /*yield*/, executeAndMeasureTime(getMetadataFromURL, [], 'fetchMetadata')];
            case 7:
                _a.sent();
                _a.label = 8;
            case 8: return [2 /*return*/];
        }
    });
}); })();
