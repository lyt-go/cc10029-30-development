import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import DeliveryView from '@/views/DeliveryView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/transfer_delivery', name: 'transfer_delivery', component: DeliveryView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
