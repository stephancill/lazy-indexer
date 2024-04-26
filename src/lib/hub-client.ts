import {
  getInsecureHubRpcClient,
  getSSLHubRpcClient,
} from '@farcaster/hub-nodejs'

const HUB_RPC = process.env.HUB_RPC
const HUB_SSL = process.env.HUB_SSL || 'true'

if (!HUB_RPC) {
  throw new Error('HUB_RPC env variable is not set')
}

export const hubClient =
  HUB_SSL === 'true'
    ? getSSLHubRpcClient(HUB_RPC)
    : getInsecureHubRpcClient(HUB_RPC)
