import type { HubRpcClient } from '@farcaster/hub-nodejs'

export type HubClient = {
  host: string
  client: HubRpcClient
}

export type ProcessResult = {
  skipped: boolean
}
