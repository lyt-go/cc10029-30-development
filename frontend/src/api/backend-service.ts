import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  DeliveryBuildResult,
  DeliveryReceipt,
  DeliveryRecordRow,
  DeliveryVerifyResult,
  EntryRow,
  OverviewResult,
  PageResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

async function errorDetail(response: Response, fallback: string): Promise<string> {
  try {
    const payload = (await response.json()) as { detail?: string; message?: string }
    return payload.detail ?? payload.message ?? fallback
  } catch {
    return fallback
  }
}

export async function listDeliveryReceipts(batch: string): Promise<DeliveryReceipt[]> {
  const query = new URLSearchParams({ batch }).toString()
  const response = await request(`/api/delivery/receipts?${query}`)
  if (!response.ok) throw new Error(await errorDetail(response, '暂存回执读取失败'))
  const payload = (await response.json()) as { items: DeliveryReceipt[] }
  return payload.items
}

export async function listDeliveries(batch: string): Promise<DeliveryRecordRow[]> {
  const query = new URLSearchParams({ batch }).toString()
  const response = await request(`/api/delivery/deliveries?${query}`)
  if (!response.ok) throw new Error(await errorDetail(response, '交付记录读取失败'))
  const payload = (await response.json()) as { items: DeliveryRecordRow[] }
  return payload.items
}

export async function buildDeliveryPackage(
  batch: string,
  options: { receiptCode?: string; landingPath?: string; confirmLandingPath?: boolean } = {},
): Promise<DeliveryBuildResult> {
  const body = {
    batch,
    receipt_code: options.receiptCode || undefined,
    landing_path: options.landingPath || undefined,
    confirm_landing_path: options.confirmLandingPath ?? false,
  }
  const response = await request('/api/delivery/package', {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) throw new Error(await errorDetail(response, '交付包打包失败'))
  return await response.json()
}

export async function downloadDeliveryPackage(result: DeliveryBuildResult): Promise<void> {
  const response = await request(result.download_url)
  if (!response.ok) throw new Error(await errorDetail(response, '交付包下载失败'))
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${result.delivery_no}.zip`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function verifyDelivery(
  deliveryNo: string,
  landing = false,
): Promise<DeliveryVerifyResult> {
  const suffix = landing ? 'landing-verify' : 'verify'
  const response = await request(`/api/delivery/${encodeURIComponent(deliveryNo)}/${suffix}`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  if (!response.ok) throw new Error(await errorDetail(response, '交付包核验失败'))
  return await response.json()
}

export async function verifyDeliveryUpload(
  file: File,
  batch?: string,
): Promise<DeliveryVerifyResult> {
  const query = batch ? `?${new URLSearchParams({ batch }).toString()}` : ''
  const response = await request(`/api/delivery/verify-upload${query}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/zip' },
    body: file,
  })
  if (!response.ok) throw new Error(await errorDetail(response, '下载件复算失败'))
  return await response.json()
}
