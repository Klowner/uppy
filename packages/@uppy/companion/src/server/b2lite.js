const request = require('request')

const B2_API_VERSION = 2
const B2_API_URL = `https://api.backblazeb2.com/b2api/v${B2_API_VERSION}/`
// request.debug = true

module.exports = class B2Lite {
  constructor (options) {
    // TODO - REMOVE THIS
    this.applicationKeyId = options.applicationKeyId || '001eb6407e78f8b0000000002'
    this.applicationKey = options.applicationKey || 'K001w5txG3p46rQmvQrQWIIXRPAgGuE'

    this.retries = 3
    // This will be populated after a successful authorization
    this.auth = {}
  }

  getURL (method) {
    if (!this.auth.apiUrl) {
      throw new Error('getURL() called before successful authorize()')
    }
    return this.auth.apiUrl + `/b2api/v${B2_API_VERSION}/${method}`
  }

  /**
   * Request new authorization key from Backblaze.
   */
  authorize () {
    return this._authorizing || (this._authorizing = new Promise((resolve, reject) => {
      const finish = (body) => {
        this._authorizing = null
      }

      request.get(B2_API_URL + 'b2_authorize_account', {
        json: true,
        auth: {
          user: this.applicationKeyId,
          pass: this.applicationKey,
          sendImmediately: false
        }
      }, (err, res, body) => {
        if (err) {
          reject(err)
        } else {
          this.auth = body
          resolve(body)
        }
        finish()
      })
    }))
  }

  /**
   * Gets an instance of `request` with 'Authorization' headers
   * set to a (hopefully) valid authorization token.
   */
  _getPreauthorizedRequest () {
    return this._authorizedRequest || (this._authorizedRequest =
      this.authorize()
        .then((authData) => {
          // Bind authorization token to new `request`
          return request.defaults({
            headers: {
              Authorization: authData.authorizationToken
            }
          })
        })
    )
  }

  apiRequest (action, params, ttl) {
    return this._getPreauthorizedRequest()
      .then((request) => new Promise((resolve, reject) => {
        const url = this.getURL(action)

        // If `params` is a function, call it and use the result
        if (typeof params === 'function') {
          params = params(this.auth, this)
        }

        request(url, params, (err, res, body) => {
          if (err) {
            // TODO -- check for token expirations and retry failures
            console.log('WE GOT AN ERROR', err)
            reject(err)
          } else {
            resolve(body)
          }
        })
      }))
      // .catch((err) => {
      //   if (err.status === 401) {
      //     if (typeof ttl === 'undefined') {
      //       ttl = this.retries
      //     }
      //     return this.apiRequest(action, params, ttl - 1)
      //   }
      //   throw err
      // })
  }

  // { bucketId, fileName, contentType }
  startLargeFile (params) {
    return this.apiRequest('b2_start_large_file', (auth) => ({
      method: 'POST',
      json: true,
      body: {
        contentType: 'b2/x-auto',
        ...params
      }
    }))
  }

  // { bucketId }
  getBucketId (params) {
    return this.apiRequest('b2_list_buckets', (auth) => ({
      method: 'POST',
      json: true,
      body: {
        accountId: auth.accountId,
        bucketName: params.bucketName
      }
    })).then(response => {
      if (response.buckets && response.buckets.length) {
        return response.buckets[0].bucketId
      }
      throw new Error('failed to get bucketId')
    })
  }

  // { bucketName (optional), bucketTypes (optional)
  listBuckets (params) {
    return this.apiRequest('b2_list_buckets', (auth) => ({
      method: 'POST',
      json: true,
      body: {
        accountId: auth.accountId,
        ...params
      }
    }))
  }

  // { fileId }
  getUploadPartURL (params) {
    return this.apiRequest('b2_get_upload_part_url', {
      method: 'POST',
      json: true,
      body: params
    })
  }
}
