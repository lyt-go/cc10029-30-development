<template>
  <section class="page" data-module="transfer_delivery">
    <header class="page-head">
      <div>
        <h2>传输交付数据包</h2>
        <p class="page-desc">
          按批号生成含实际分片校验和暂存回执的交付包；成包前确认采用的新落地路径，下载后逐片复算。
        </p>
      </div>
    </header>

    <form class="analysis-form" @submit.prevent="loadReceipts">
      <label>
        <span>批次编号</span>
        <input v-model="batch" data-testid="delivery-batch" placeholder="只输入批号即可" required />
      </label>
      <button class="btn" data-testid="load-receipts" type="submit">读取暂存回执</button>
    </form>

    <p v-if="errorMessage" class="error-text" data-testid="delivery-error">{{ errorMessage }}</p>
    <p v-if="notice" class="page-desc" data-testid="delivery-notice">{{ notice }}</p>

    <table v-if="receipts.length" class="data-table">
      <thead>
        <tr>
          <th>回执编号</th>
          <th>落地路径</th>
          <th>分片数</th>
          <th>记录条数</th>
          <th>确认时间</th>
          <th>采用</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="receipt in receipts" :key="receipt.receipt_code" :data-testid="`receipt-${receipt.receipt_code}`">
          <td>{{ receipt.receipt_code }}</td>
          <td>{{ receipt.landing_path }}</td>
          <td>{{ receipt.chunk_count }}</td>
          <td>{{ receipt.record_count }}</td>
          <td>{{ formatTime(receipt.confirmed_at) }}</td>
          <td>
            <label class="legend-item">
              <input v-model="selectedReceipt" type="radio" :value="receipt.receipt_code" />
              采用
            </label>
          </td>
        </tr>
      </tbody>
    </table>

    <form class="analysis-form" @submit.prevent="buildPackage">
      <label>
        <span>确认新落地路径（成包前）</span>
        <input v-model="landingPath" data-testid="delivery-landing-path" placeholder="留空则采用回执自身路径" />
      </label>
      <label class="legend-item">
        <input v-model="confirmLandingPath" type="checkbox" data-testid="delivery-confirm-path" />
        按新路径确认
      </label>
      <button class="btn primary" data-testid="delivery-build" type="submit" :disabled="building">
        {{ building ? '打包中…' : '生成交付包' }}
      </button>
    </form>

    <div v-if="build" class="analysis-card" data-testid="delivery-build-result">
      <strong>已生成交付包：{{ build.delivery_no }}</strong>
      <p class="page-desc">
        采用回执 {{ build.adopted_receipt.receipt_code }}（{{ build.adopted_receipt.landing_path }}），
        {{ build.chunk_count }} 个分片 / {{ build.record_count }} 条 / {{ build.total_bytes }} 字节。
      </p>
      <p class="page-desc">交付校验值：{{ build.delivery_checksum }}</p>
      <button class="btn" type="button" data-testid="delivery-download" @click="download">
        下载交付包
      </button>
    </div>

    <h3>交付记录</h3>
    <table class="data-table" data-testid="delivery-records">
      <thead>
        <tr>
          <th>交付编号</th>
          <th>批次编号</th>
          <th>采用回执</th>
          <th>落地路径</th>
          <th>分片数量</th>
          <th>记录条数</th>
          <th>交付状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in deliveries" :key="row.id" :data-testid="`delivery-row-${row.id}`">
          <td>{{ row['交付编号'] }}</td>
          <td>{{ row['批次编号'] }}</td>
          <td>{{ row['采用回执'] }}</td>
          <td>{{ row['落地路径'] }}</td>
          <td>{{ row['分片数量'] }}</td>
          <td>{{ row['记录条数'] }}</td>
          <td>{{ row['交付状态'] }}</td>
          <td class="row-actions">
            <button class="link" type="button" @click="pickDelivery(row)">核验</button>
          </td>
        </tr>
        <tr v-if="!deliveries.length">
          <td colspan="8" class="empty-state">该批次暂无正式交付记录</td>
        </tr>
      </tbody>
    </table>

    <form class="analysis-form" @submit.prevent="verifyByNumber">
      <label>
        <span>按交付号核验（中断重下后仍可核验）</span>
        <input v-model="deliveryNo" data-testid="delivery-no" placeholder="DELI-…" />
      </label>
      <button class="btn" type="submit" data-testid="delivery-verify-number">核验已交付包</button>
    </form>

    <form class="analysis-form" @submit.prevent="verifyUpload">
      <label>
        <span>用下载件复算校验</span>
        <input type="file" accept=".zip" data-testid="delivery-upload" @change="onFileChange" />
      </label>
      <button class="btn" type="submit" data-testid="delivery-verify-upload" :disabled="!uploadFile">
        复算下载件
      </button>
      <button class="btn ghost" type="button" data-testid="delivery-landing-verify" @click="landingVerify">
        目标端落地校验
      </button>
    </form>

    <template v-if="verify">
      <div class="analysis-summary" data-testid="delivery-verify-result">
        <article class="analysis-card">
          <span class="stat-label">交付包核验</span>
          <strong :class="verify.ok ? 'trend-up' : 'trend-down'">{{ verify.ok ? '通过' : '未通过' }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">清单校验值</span>
          <strong class="stat-value">{{ verify.manifest_ok ? '一致' : '不一致' }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">逐片复算</span>
          <strong class="stat-value">{{ verify.all_chunks_ok ? '全部通过' : '存在差异' }}</strong>
        </article>
        <article class="analysis-card">
          <span class="stat-label">另一轮校验</span>
          <strong class="stat-value" data-testid="delivery-another-round">
            {{ verify.another_round_required ? '需要' : '不需要' }}
          </strong>
        </article>
      </div>

      <p class="page-desc" data-testid="delivery-conclusion">{{ verify.landing_conclusion }}</p>

      <h3>采用回执关联的实际分片</h3>
      <table class="data-table" data-testid="delivery-receipt-association">
        <thead>
          <tr>
            <th>分片</th>
            <th>回执路径</th>
            <th>回执校验值</th>
            <th>包内路径</th>
            <th>包内校验值</th>
            <th>关联</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in verify.receipt_chunk_association.chunks" :key="item.chunk_index">
            <td>{{ item.chunk_index }}</td>
            <td>{{ item.receipt_path }}</td>
            <td class="mono">{{ shortHash(item.receipt_sha256) }}</td>
            <td>{{ item.package_path }}</td>
            <td class="mono">{{ shortHash(item.package_sha256) }}</td>
            <td>{{ item.linked ? '一致' : '不一致' }}</td>
          </tr>
        </tbody>
      </table>

      <h3>逐片复算</h3>
      <table class="data-table" data-testid="delivery-chunk-checks">
        <thead>
          <tr>
            <th>分片</th>
            <th>清单路径</th>
            <th>清单条数</th>
            <th>实际条数</th>
            <th>清单校验值</th>
            <th>实际校验值</th>
            <th>结论</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in verify.chunks" :key="item.chunk_index">
            <td>{{ item.chunk_index }}</td>
            <td>{{ item.path }}</td>
            <td>{{ item.expected_record_count }}</td>
            <td>{{ item.actual_record_count ?? '—' }}</td>
            <td class="mono">{{ shortHash(item.expected_sha256) }}</td>
            <td class="mono">{{ shortHash(item.actual_sha256) }}</td>
            <td>{{ item.ok ? '一致' : '不一致' }}</td>
          </tr>
        </tbody>
      </table>

      <h3>后续待校验范围</h3>
      <table class="data-table" data-testid="delivery-pending-scope">
        <thead>
          <tr>
            <th>回执编号</th>
            <th>落地路径</th>
            <th>来源</th>
            <th>分片数</th>
            <th>记录条数</th>
            <th>分片可读</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in verify.pending_scope.newer_receipts" :key="item.receipt_code">
            <td>{{ item.receipt_code }}</td>
            <td>{{ item.landing_path }}</td>
            <td>{{ item.source === 'package_record' ? '包内记录' : '当前回执账' }}</td>
            <td>{{ item.chunk_count }}</td>
            <td>{{ item.record_count }}</td>
            <td>{{ item.chunks_readable ? '是' : '否' }}</td>
          </tr>
          <tr v-if="!verify.pending_scope.newer_receipts.length">
            <td colspan="6" class="empty-state">采用回执为该批次最新回执，无后续待校验范围</td>
          </tr>
        </tbody>
      </table>
    </template>
  </section>
</template>

<script setup lang="ts">
import { ref } from 'vue'

import {
  buildDeliveryPackage,
  downloadDeliveryPackage,
  listDeliveries,
  listDeliveryReceipts,
  verifyDelivery,
  verifyDeliveryUpload,
} from '@/api/backend-service'
import type {
  DeliveryBuildResult,
  DeliveryReceipt,
  DeliveryRecordRow,
  DeliveryVerifyResult,
} from '@/data/types'

const batch = ref('')
const receipts = ref<DeliveryReceipt[]>([])
const deliveries = ref<DeliveryRecordRow[]>([])
const selectedReceipt = ref('')
const landingPath = ref('')
const confirmLandingPath = ref(false)
const building = ref(false)
const build = ref<DeliveryBuildResult | null>(null)
const deliveryNo = ref('')
const verify = ref<DeliveryVerifyResult | null>(null)
const uploadFile = ref<File | null>(null)
const errorMessage = ref('')
const notice = ref('')

function formatTime(epoch: number): string {
  if (!epoch) return '—'
  return new Date(epoch * 1000).toLocaleString()
}

function shortHash(value?: string): string {
  if (!value) return '—'
  return value.length > 18 ? `${value.slice(0, 10)}…${value.slice(-6)}` : value
}

async function loadReceipts() {
  errorMessage.value = ''
  notice.value = ''
  verify.value = null
  try {
    receipts.value = await listDeliveryReceipts(batch.value.trim())
    deliveries.value = await listDeliveries(batch.value.trim())
    if (!receipts.value.length) {
      notice.value = '该批次还没有暂存确认回执，请先完成暂存落地确认。'
    } else {
      selectedReceipt.value = receipts.value[0].receipt_code
      landingPath.value = receipts.value[0].landing_path
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '暂存回执读取失败'
  }
}

async function buildPackage() {
  errorMessage.value = ''
  building.value = true
  try {
    build.value = await buildDeliveryPackage(batch.value.trim(), {
      receiptCode: selectedReceipt.value || undefined,
      landingPath: landingPath.value.trim() || undefined,
      confirmLandingPath: confirmLandingPath.value,
    })
    deliveryNo.value = build.value.delivery_no
    notice.value = '交付包已正式生成，可按交付号下载或核验。'
    deliveries.value = await listDeliveries(batch.value.trim())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付包打包失败'
  } finally {
    building.value = false
  }
}

function pickDelivery(row: DeliveryRecordRow) {
  deliveryNo.value = row['交付编号']
  void verifyByNumber()
}

async function download() {
  if (!build.value) return
  errorMessage.value = ''
  try {
    await downloadDeliveryPackage(build.value)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付包下载失败'
  }
}

async function verifyByNumber() {
  errorMessage.value = ''
  try {
    verify.value = await verifyDelivery(deliveryNo.value.trim(), false)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付包核验失败'
  }
}

async function landingVerify() {
  if (!deliveryNo.value.trim()) {
    errorMessage.value = '请填写交付号后再做落地校验'
    return
  }
  errorMessage.value = ''
  try {
    verify.value = await verifyDelivery(deliveryNo.value.trim(), true)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '落地校验失败'
  }
}

function onFileChange(event: Event) {
  const target = event.target as HTMLInputElement
  uploadFile.value = target.files?.[0] ?? null
}

async function verifyUpload() {
  if (!uploadFile.value) return
  errorMessage.value = ''
  try {
    verify.value = await verifyDeliveryUpload(uploadFile.value, batch.value.trim() || undefined)
    deliveryNo.value = verify.value.delivery_no || deliveryNo.value
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载件复算失败'
  }
}
</script>
