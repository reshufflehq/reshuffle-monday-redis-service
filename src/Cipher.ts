import crypto from 'crypto'

// Symmetric text encryption
//
export class Cipher {
  private static readonly KEY_LENGTH = 32
  private static readonly keyRegex = new RegExp(
    `^[0-9a-fA-F]{${Cipher.KEY_LENGTH * 2}}$`,
  )

  private key: Buffer

  // Create a new cipher with the specified encryption key.
  //
  // @param key encryption key (32 bytes)
  //
  constructor(key: string | Buffer) {
    if (typeof key === 'string' && Cipher.keyRegex.test(key)) {
      this.key = Buffer.from(key, 'hex')
    } else if (
      typeof key === 'object' &&
      key instanceof Buffer &&
      key.length === Cipher.KEY_LENGTH
    ) {
      this.key = key
    } else {
      throw new Error(`Invalid encryption key: ${key}`)
    }
  }

  // Encrypt.
  //
  // @param text clear text string
  //
  // @return encrypted string
  //
  public encrypt(text: string): string {
    const iv = crypto.randomBytes(16)
    const cipher = crypto.createCipheriv('aes256', this.key, iv)
    const ct = cipher.update(text, 'utf8', 'hex') + cipher.final('hex')
    return JSON.stringify({ iv, ct })
  }

  // Decrypt.
  //
  // @param encoded encrypted string produced by encrypt()
  //
  // @return clear text string
  //
  public decrypt(encoded: string): string {
    const { iv, ct } = JSON.parse(encoded)
    const ivb = Buffer.from(iv, 'hex')
    const decipher = crypto.createDecipheriv('aes256', this.key, ivb)
    return decipher.update(ct, 'hex', 'utf8') + decipher.final('utf8')
  }
}
