import fs from 'fs/promises'
import * as crypto from 'crypto'
import * as stream from 'stream'

import * as github from '@actions/github'
import * as core from '@actions/core'
import * as httpClient from '@actions/http-client'
import unzip from 'unzip-stream'
import {
  DownloadArtifactOptions,
  DownloadArtifactResponse,
  StreamExtractResponse
} from '../shared/interfaces'
import { getUserAgentString } from '../shared/user-agent'
import { getGitHubWorkspaceDir } from '../shared/config'
import { internalArtifactTwirpClient } from '../shared/artifact-twirp-client'
import {
  GetSignedArtifactURLRequest,
  Int64Value,
  ListArtifactsRequest
} from '../../generated'
import { getBackendIdsFromToken } from '../shared/util'
import { ArtifactNotFoundError } from '../shared/errors'

const scrubQueryParameters = (url: string): string => {
  const parsed = new URL(url)
  parsed.search = ''
  return parsed.toString()
}

async function exists(path: string): Promise<boolean> {
  try {
    await fs.access(path)
    return true
  } catch (error) {
    if (error.code === 'ENOENT') {
      return false
    } else {
      throw error
    }
  }
}

async function streamExtract(
  url: string,
  directory: string
): Promise<StreamExtractResponse> {
  core.info(`Starting streamExtract for URL: ${scrubQueryParameters(url)}`)
  core.info(`Target directory: ${directory}`)

  let retryCount = 0
  while (retryCount < 5) {
    try {
      core.info(`Attempt ${retryCount + 1}/5 to download and extract artifact`)
      const result = await streamExtractExternal(url, directory)
      core.info(`StreamExtract completed successfully on attempt ${retryCount + 1}`)
      return result
    } catch (error) {
      retryCount++
      core.warning(
        `Failed to download artifact after ${retryCount} retries due to ${error.message}. Retrying in 5 seconds...`
      )
      core.debug(`Error details: ${error.stack}`)
      // wait 5 seconds before retrying
      await new Promise(resolve => setTimeout(resolve, 5000))
    }
  }

  const errorMessage = `Artifact download failed after ${retryCount} retries.`
  core.error(errorMessage)
  throw new Error(errorMessage)
}

export async function streamExtractExternal(
  url: string,
  directory: string
): Promise<StreamExtractResponse> {
  core.info(`Initializing HTTP client for artifact download`)
  const client = new httpClient.HttpClient(getUserAgentString())

  core.info(`Making HTTP GET request to blob storage`)
  const response = await client.get(url)
  core.info(`HTTP response received with status: ${response.message.statusCode}`)

  if (response.message.statusCode !== 200) {
    const errorMessage = `Unexpected HTTP response from blob storage: ${response.message.statusCode} ${response.message.statusMessage}`
    core.error(errorMessage)
    throw new Error(errorMessage)
  }

  const timeout = 30 * 1000 // 30 seconds
  core.debug(`Setting timeout for blob storage chunks: ${timeout}ms`)
  let sha256Digest: string | undefined = undefined

  return new Promise((resolve, reject) => {
    const timerFn = (): void => {
      core.error(`Timeout reached: Blob storage chunk did not respond in ${timeout}ms`)
      response.message.destroy(
        new Error(`Blob storage chunk did not respond in ${timeout}ms`)
      )
    }
    let timer = setTimeout(timerFn, timeout)

    core.info(`Creating hash stream for SHA256 verification`)
    const hashStream = crypto.createHash('sha256').setEncoding('hex')
    const passThrough = new stream.PassThrough()

    core.info(`Setting up stream pipeline for download and extraction`)
    response.message.pipe(passThrough)
    passThrough.pipe(hashStream)
    const extractStream = passThrough

    let dataChunksReceived = 0
    extractStream
      .on('data', (chunk) => {
        dataChunksReceived++
        if (dataChunksReceived % 100 === 0) {
          core.debug(`Received ${dataChunksReceived} data chunks (chunk size: ${chunk.length} bytes)`)
        }
        clearTimeout(timer)
        timer = setTimeout(timerFn, timeout)
      })
      .on('error', (error: Error) => {
        core.error(
          `Stream error during artifact download: ${error.message}`
        )
        core.debug(`Stream error stack trace: ${error.stack}`)
        clearTimeout(timer)
        reject(error)
      })
      .pipe(unzip.Extract({ path: directory }))
      .on('close', () => {
        core.info(`Extraction completed successfully`)
        core.info(`Total data chunks received: ${dataChunksReceived}`)
        clearTimeout(timer)
        if (hashStream) {
          hashStream.end()
          sha256Digest = hashStream.read() as string
          core.info(`SHA256 digest of downloaded artifact is ${sha256Digest}`)
        }
        resolve({ sha256Digest: `sha256:${sha256Digest}` })
      })
      .on('error', (error: Error) => {
        core.error(`Extraction error: ${error.message}`)
        core.debug(`Extraction error stack trace: ${error.stack}`)
        clearTimeout(timer)
        reject(error)
      })
  })
}

export async function downloadArtifactPublic(
  artifactId: number,
  repositoryOwner: string,
  repositoryName: string,
  token: string,
  options?: DownloadArtifactOptions
): Promise<DownloadArtifactResponse> {
  core.info(`Starting public artifact download`)
  core.info(`Artifact ID: ${artifactId}`)
  core.info(`Repository: ${repositoryOwner}/${repositoryName}`)
  core.info(`Options: ${JSON.stringify(options || {})}`)

  const downloadPath = await resolveOrCreateDirectory(options?.path)
  core.info(`Download path resolved to: ${downloadPath}`)

  core.info(`Initializing GitHub API client`)
  const api = github.getOctokit(token)

  let digestMismatch = false

  core.info(
    `Downloading artifact '${artifactId}' from '${repositoryOwner}/${repositoryName}'`
  )

  core.info(`Making API call to download artifact`)
  const { headers, status } = await api.rest.actions.downloadArtifact({
    owner: repositoryOwner,
    repo: repositoryName,
    artifact_id: artifactId,
    archive_format: 'zip',
    request: {
      redirect: 'manual'
    }
  })

  core.info(`API response status: ${status}`)
  core.debug(`Response headers: ${JSON.stringify(headers)}`)

  if (status !== 302) {
    const errorMessage = `Unable to download artifact. Unexpected status: ${status}`
    core.error(errorMessage)
    throw new Error(errorMessage)
  }

  const { location } = headers
  if (!location) {
    const errorMessage = `Unable to redirect to artifact download url`
    core.error(errorMessage)
    core.debug(`Headers received: ${JSON.stringify(headers)}`)
    throw new Error(errorMessage)
  }

  core.info(
    `Redirecting to blob download url: ${scrubQueryParameters(location)}`
  )

  try {
    core.info(`Starting download of artifact to: ${downloadPath}`)
    const extractResponse = await streamExtract(location, downloadPath)
    core.info(`Artifact download completed successfully.`)
    core.info(`Final SHA256 digest: ${extractResponse.sha256Digest}`)

    if (options?.expectedHash) {
      core.info(`Verifying artifact integrity`)
      core.info(`Expected hash: ${options.expectedHash}`)
      core.info(`Computed hash: ${extractResponse.sha256Digest}`)

      if (options?.expectedHash !== extractResponse.sha256Digest) {
        digestMismatch = true
        core.warning(`Digest mismatch detected!`)
        core.debug(`Computed digest: ${extractResponse.sha256Digest}`)
        core.debug(`Expected digest: ${options.expectedHash}`)
      } else {
        core.info(`Artifact integrity verification passed`)
      }
    } else {
      core.debug(`No expected hash provided, skipping integrity verification`)
    }
  } catch (error) {
    const errorMessage = `Unable to download and extract artifact: ${error.message}`
    core.error(errorMessage)
    core.debug(`Download error stack trace: ${error.stack}`)
    throw new Error(errorMessage)
  }

  core.info(`Download operation completed. Digest mismatch: ${digestMismatch}`)
  return { downloadPath, digestMismatch }
}

export async function downloadArtifactInternal(
  artifactId: number,
  options?: DownloadArtifactOptions
): Promise<DownloadArtifactResponse> {
  core.info(`Starting internal artifact download`)
  core.info(`Artifact ID: ${artifactId}`)
  core.info(`Options: ${JSON.stringify(options || {})}`)

  const downloadPath = await resolveOrCreateDirectory(options?.path)
  core.info(`Download path resolved to: ${downloadPath}`)

  core.info(`Initializing internal artifact client`)
  const artifactClient = internalArtifactTwirpClient()

  let digestMismatch = false

  core.info(`Getting backend IDs from token`)
  const { workflowRunBackendId, workflowJobRunBackendId } =
    getBackendIdsFromToken()

  core.debug(`Workflow run backend ID: ${workflowRunBackendId}`)
  core.debug(`Workflow job run backend ID: ${workflowJobRunBackendId}`)

  const listReq: ListArtifactsRequest = {
    workflowRunBackendId,
    workflowJobRunBackendId,
    idFilter: Int64Value.create({ value: artifactId.toString() })
  }

  core.info(`Listing artifacts with filter for ID: ${artifactId}`)
  const { artifacts } = await artifactClient.ListArtifacts(listReq)
  core.info(`Found ${artifacts.length} artifacts matching the filter`)

  if (artifacts.length === 0) {
    const errorMessage = `No artifacts found for ID: ${artifactId}\nAre you trying to download from a different run? Try specifying a github-token with \`actions:read\` scope.`
    core.error(errorMessage)
    throw new ArtifactNotFoundError(errorMessage)
  }

  if (artifacts.length > 1) {
    core.warning('Multiple artifacts found, defaulting to first.')
    core.debug(`Artifacts found: ${artifacts.map(a => `${a.name} (ID: ${a.databaseId})`).join(', ')}`)
  }

  const selectedArtifact = artifacts[0]
  core.info(`Selected artifact: ${selectedArtifact.name}`)
  core.debug(`Artifact details: ${JSON.stringify({
    name: selectedArtifact.name,
    databaseId: selectedArtifact.databaseId,
    workflowRunBackendId: selectedArtifact.workflowRunBackendId,
    workflowJobRunBackendId: selectedArtifact.workflowJobRunBackendId
  })}`)

  const signedReq: GetSignedArtifactURLRequest = {
    workflowRunBackendId: selectedArtifact.workflowRunBackendId,
    workflowJobRunBackendId: selectedArtifact.workflowJobRunBackendId,
    name: selectedArtifact.name
  }

  core.info(`Requesting signed URL for artifact: ${selectedArtifact.name}`)
  const { signedUrl } = await artifactClient.GetSignedArtifactURL(signedReq)

  core.info(
    `Redirecting to blob download url: ${scrubQueryParameters(signedUrl)}`
  )

  try {
    core.info(`Starting download of artifact to: ${downloadPath}`)
    const extractResponse = await streamExtract(signedUrl, downloadPath)
    core.info(`Artifact download completed successfully.`)
    core.info(`Final SHA256 digest: ${extractResponse.sha256Digest}`)

    if (options?.expectedHash) {
      core.info(`Verifying artifact integrity`)
      core.info(`Expected hash: ${options.expectedHash}`)
      core.info(`Computed hash: ${extractResponse.sha256Digest}`)

      if (options?.expectedHash !== extractResponse.sha256Digest) {
        digestMismatch = true
        core.warning(`Digest mismatch detected!`)
        core.debug(`Computed digest: ${extractResponse.sha256Digest}`)
        core.debug(`Expected digest: ${options.expectedHash}`)
      } else {
        core.info(`Artifact integrity verification passed`)
      }
    } else {
      core.debug(`No expected hash provided, skipping integrity verification`)
    }
  } catch (error) {
    const errorMessage = `Unable to download and extract artifact: ${error.message}`
    core.error(errorMessage)
    core.debug(`Download error stack trace: ${error.stack}`)
    throw new Error(errorMessage)
  }

  core.info(`Download operation completed. Digest mismatch: ${digestMismatch}`)
  return { downloadPath, digestMismatch }
}

async function resolveOrCreateDirectory(
  downloadPath = getGitHubWorkspaceDir()
): Promise<string> {
  core.info(`Resolving download directory: ${downloadPath}`)

  if (!(await exists(downloadPath))) {
    core.info(
      `Artifact destination folder does not exist, creating: ${downloadPath}`
    )
    await fs.mkdir(downloadPath, { recursive: true })
    core.info(`Successfully created directory: ${downloadPath}`)
  } else {
    core.debug(`Artifact destination folder already exists: ${downloadPath}`)
  }

  core.info(`Download directory resolved: ${downloadPath}`)
  return downloadPath
}
