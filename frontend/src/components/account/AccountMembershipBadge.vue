<template>
  <div class="inline-flex min-w-[5rem] flex-col gap-1 text-xs font-medium">
    <span :class="['inline-flex w-fit items-center gap-1 rounded px-1.5 py-1', planBadgeClass]">
      <GrokFreeIcon v-if="isGrokFreePlan" data-testid="grok-free-plan-icon" />
      <Icon v-else-if="planIconName" :name="planIconName" size="xs" aria-hidden="true" />
      <span>{{ planLabel }}</span>
    </span>

    <span
      v-if="privacyBadge"
      :class="['inline-flex w-fit items-center gap-1 rounded px-1.5 py-0.5 text-[11px]', privacyBadge.class]"
      :title="privacyBadge.title"
    >
      <svg class="h-3 w-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
        <path stroke-linecap="round" stroke-linejoin="round" :d="privacyBadge.icon" />
      </svg>
      <span>{{ privacyBadge.label }}</span>
    </span>

    <span v-if="expiresLabel" class="pl-0.5 text-[10px] leading-tight text-gray-400 dark:text-gray-500" :title="subscriptionExpiresAt">
      {{ expiresLabel }}
    </span>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import type { AccountPlatform, AccountType } from '@/types'
import Icon from '@/components/icons/Icon.vue'
import GrokFreeIcon from '@/components/common/GrokFreeIcon.vue'

const { t } = useI18n()

interface Props {
  platform: AccountPlatform
  type: AccountType
  planType?: string
  privacyMode?: string
  subscriptionExpiresAt?: string
}

const props = defineProps<Props>()

const normalizedPlanType = computed(() =>
  (props.planType || '').trim().toLowerCase().replace(/[\s_-]+/g, '')
)

const planLabel = computed(() => {
  if (!normalizedPlanType.value) return t('admin.accounts.membership.unknown')
  switch (normalizedPlanType.value) {
    case 'plus':
      return 'Plus'
    case 'team':
      return 'Team'
    case 'chatgptpro':
    case 'pro':
      return 'Pro'
    case 'free':
    case 'basic':
      return props.platform === 'grok' ? 'Grok Free' : 'Free'
    case 'supergrok':
      return 'SuperGrok'
    case 'supergrokheavy':
      return 'SuperGrok Heavy'
    case 'heavy':
      return 'Heavy'
    case 'abnormal':
      return t('admin.accounts.subscriptionAbnormal')
    default:
      return props.planType
  }
})

const isGrokFreePlan = computed(() =>
  props.platform === 'grok' &&
  (normalizedPlanType.value === 'free' || normalizedPlanType.value === 'basic')
)

const planIconName = computed<'bolt' | null>(() => {
  if (props.platform !== 'grok') return null
  if (
    normalizedPlanType.value === 'supergrok' ||
    normalizedPlanType.value === 'supergrokheavy' ||
    normalizedPlanType.value === 'heavy' ||
    normalizedPlanType.value.includes('heavy')
  ) {
    return 'bolt'
  }
  return null
})

const planBadgeClass = computed(() => {
  if (!normalizedPlanType.value) {
    return 'bg-slate-100 text-slate-500 dark:bg-slate-800 dark:text-slate-400'
  }
  if (normalizedPlanType.value === 'abnormal') {
    return 'bg-red-100 text-red-600 dark:bg-red-900/30 dark:text-red-400'
  }
  if (normalizedPlanType.value === 'free' || normalizedPlanType.value === 'basic') {
    return 'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300'
  }
  if (props.platform === 'grok' && normalizedPlanType.value) {
    if (normalizedPlanType.value.includes('heavy')) {
      return 'bg-purple-100 text-purple-600 dark:bg-purple-900/30 dark:text-purple-300'
    }
    if (normalizedPlanType.value.includes('supergrok')) {
      return 'bg-cyan-100 text-cyan-700 dark:bg-cyan-900/30 dark:text-cyan-300'
    }
    return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
  }
  if (normalizedPlanType.value === 'plus') {
    return 'bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-300'
  }
  if (normalizedPlanType.value === 'team') {
    return 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300'
  }
  if (normalizedPlanType.value === 'pro' || normalizedPlanType.value === 'chatgptpro') {
    return 'bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300'
  }
  return 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300'
})

const expiresLabel = computed(() => {
  if (!props.subscriptionExpiresAt || !props.planType) return ''
  if (normalizedPlanType.value === 'free' || normalizedPlanType.value === 'basic') return ''
  try {
    const d = new Date(props.subscriptionExpiresAt)
    if (Number.isNaN(d.getTime())) return ''
    const yyyy = d.getFullYear()
    const mm = String(d.getMonth() + 1).padStart(2, '0')
    const dd = String(d.getDate()).padStart(2, '0')
    return `${t('admin.accounts.subscriptionExpires')} ${yyyy}-${mm}-${dd}`
  } catch {
    return ''
  }
})

const privacyBadge = computed(() => {
  if (props.type !== 'oauth' || !props.privacyMode) return null
  if (props.platform !== 'openai' && props.platform !== 'antigravity') return null
  const shieldCheck = 'M9 12.75L11.25 15 15 9.75m-3-7.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285z'
  const shieldX = 'M12 9v3.75m0-10.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285zM12 18h.008v.008H12V18z'
  switch (props.privacyMode) {
    case 'enabled':
    case 'always_allow':
      return {
        label: t('admin.accounts.privacyOptOut.enabledShort'),
        title: t('admin.accounts.privacyOptOut.enabled'),
        class: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
        icon: shieldCheck
      }
    case 'disabled':
    case 'always_deny':
      return {
        label: t('admin.accounts.privacyOptOut.disabledShort'),
        title: t('admin.accounts.privacyOptOut.disabled'),
        class: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
        icon: shieldX
      }
    case 'unknown':
      return {
        label: t('admin.accounts.privacyOptOut.unknownShort'),
        title: t('admin.accounts.privacyOptOut.unknown'),
        class: 'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300',
        icon: shieldX
      }
    default:
      return null
  }
})
</script>
