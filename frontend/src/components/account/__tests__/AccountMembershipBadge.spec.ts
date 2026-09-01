import { describe, expect, it, vi } from 'vitest'
import { mount } from '@vue/test-utils'
import AccountMembershipBadge from '../AccountMembershipBadge.vue'

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string) => key
    })
  }
})

describe('AccountMembershipBadge', () => {
  it('shows OpenAI Plus as membership without platform/type text', () => {
    const wrapper = mount(AccountMembershipBadge, {
      props: {
        platform: 'openai',
        type: 'oauth',
        planType: 'plus'
      },
      global: {
        stubs: { Icon: true }
      }
    })

    expect(wrapper.text()).toContain('Plus')
    expect(wrapper.text()).not.toContain('OpenAI')
    expect(wrapper.text()).not.toContain('OAuth')
  })

  it('shows unknown when no plan was detected', () => {
    const wrapper = mount(AccountMembershipBadge, {
      props: {
        platform: 'openai',
        type: 'oauth'
      },
      global: {
        stubs: { Icon: true }
      }
    })

    expect(wrapper.text()).toContain('admin.accounts.membership.unknown')
  })

  it('shows subscription expiration for paid plans only', () => {
    const paid = mount(AccountMembershipBadge, {
      props: {
        platform: 'openai',
        type: 'oauth',
        planType: 'plus',
        subscriptionExpiresAt: '2026-09-15T00:00:00Z'
      },
      global: { stubs: { Icon: true } }
    })
    const free = mount(AccountMembershipBadge, {
      props: {
        platform: 'openai',
        type: 'oauth',
        planType: 'free',
        subscriptionExpiresAt: '2026-09-15T00:00:00Z'
      },
      global: { stubs: { Icon: true } }
    })

    expect(paid.text()).toContain('admin.accounts.subscriptionExpires')
    expect(free.text()).not.toContain('admin.accounts.subscriptionExpires')
  })
})
