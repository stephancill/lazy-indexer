import {
  getInsecureHubRpcClient,
  getSSLHubRpcClient,
} from '@farcaster/hub-nodejs'

import type { HubClient } from './types'

// https://github.com/farcasterxyz/hub-monorepo/commit/fb54ef89bb4731002cf3d7f59c8e52b011e58310
const MAX_RECEIVE_MESSAGE_LENGTH = 10 * 1024 * 1024 // 10mb
const defaultOptions = {
  'grpc.max_receive_message_length': MAX_RECEIVE_MESSAGE_LENGTH,
}

const HUB_RPC = process.env.HUB_RPC
const HUB_SSL = process.env.HUB_SSL || 'true'

if (!HUB_RPC) {
  throw new Error('HUB_RPC env variable is not set')
}

export const hubClient =
  HUB_SSL === 'true'
    ? getSSLHubRpcClient(HUB_RPC, defaultOptions)
    : getInsecureHubRpcClient(HUB_RPC, defaultOptions)

export const hubClientWithHost: HubClient = {
  client: hubClient,
  host: HUB_RPC,
}
