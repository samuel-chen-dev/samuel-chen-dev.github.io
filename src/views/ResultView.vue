<template>
  <div class="result">
    <!-- 彩带动画 -->
    <div class="confetti-container">
      <div
        v-for="i in 40"
        :key="i"
        class="confetti"
        :style="confettiStyle(i)"
      ></div>
    </div>

    <div class="result-content">
      <!-- 通关证书 -->
      <div class="certificate">
        <div class="cert-border">
          <div class="cert-inner">
            <div class="cert-trophy">🏆</div>
            <h1 class="cert-title">传奇探险家</h1>
            <div class="cert-divider">
              <span class="divider-line"></span>
              <span class="divider-gem">💎</span>
              <span class="divider-line"></span>
            </div>
            <p class="cert-text">
              恭喜你成功通过【失落翡翠之眼】古堡探险的全部考验！
            </p>
            <p class="cert-text">
              你展现了非凡的英语语法能力，解开了古堡的层层封印，
              最终获得了传说中的翡翠之眼。
            </p>
            <div class="cert-meta">
              <p class="cert-date">{{ currentDate }}</p>
              <p class="cert-time">{{ currentTime }}</p>
            </div>

            <!-- 水印 -->
            <div class="watermark">
              扫码添加老师微信，领取高清版证书及下一关密钥
            </div>
          </div>
        </div>
      </div>

      <!-- 按钮区 -->
      <div class="action-buttons">
        <button class="wechat-btn" @click="showWechatModal = true">
          添加老师微信
        </button>
        <button class="restart-btn" @click="restartGame">
          重新挑战
        </button>
      </div>
    </div>

    <!-- 微信弹窗 -->
    <Transition name="modal">
      <div v-if="showWechatModal" class="modal-overlay" @click.self="showWechatModal = false">
        <div class="wechat-modal">
          <h3 class="wechat-title">添加老师微信</h3>
          <p class="wechat-subtitle">领取高清版证书及更多学习资料</p>

          <div class="wechat-id-box">
            <span class="wechat-label">微信号</span>
            <span class="wechat-id">teacher_english_2025</span>
          </div>

          <button class="copy-btn" @click="copyWechat">
            {{ copied ? '已复制！' : '一键复制微信号' }}
          </button>

          <button class="close-btn" @click="showWechatModal = false">关闭</button>
        </div>
      </div>
    </Transition>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRouter } from 'vue-router'
import { useGameStore } from '../store/gameStore'

const router = useRouter()
const store = useGameStore()

const showWechatModal = ref(false)
const copied = ref(false)

const colors = ['#c9a84c', '#8b0000', '#e8c864', '#ff6b6b', '#4caf50', '#a090b0']

const currentDate = computed(() => {
  const now = new Date()
  return `${now.getFullYear()}年${now.getMonth() + 1}月${now.getDate()}日`
})

const currentTime = computed(() => {
  const now = new Date()
  return `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`
})

function confettiStyle(i) {
  return {
    left: `${Math.random() * 100}%`,
    top: `-${Math.random() * 20}px`,
    width: `${Math.random() * 10 + 6}px`,
    height: `${Math.random() * 14 + 8}px`,
    background: colors[i % colors.length],
    animationDelay: `${Math.random() * 3}s`,
    animationDuration: `${Math.random() * 2 + 3}s`,
    transform: `rotate(${Math.random() * 360}deg)`,
    opacity: Math.random() * 0.6 + 0.4,
  }
}

async function copyWechat() {
  try {
    await navigator.clipboard.writeText('teacher_english_2025')
    copied.value = true
    setTimeout(() => { copied.value = false }, 2000)
  } catch {
    // 降级方案
    const textarea = document.createElement('textarea')
    textarea.value = 'teacher_english_2025'
    document.body.appendChild(textarea)
    textarea.select()
    document.execCommand('copy')
    document.body.removeChild(textarea)
    copied.value = true
    setTimeout(() => { copied.value = false }, 2000)
  }
}

function restartGame() {
  store.resetGame()
  router.push('/')
}
</script>

<style scoped>
.result {
  position: relative;
  min-height: 100vh;
  background: radial-gradient(ellipse at center, #2a1050 0%, #1a0a2e 50%, #0d0518 100%);
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  padding: 20px;
}

/* 彩带 */
.confetti-container {
  position: fixed;
  inset: 0;
  pointer-events: none;
  z-index: 1;
  overflow: hidden;
}

.confetti {
  position: absolute;
  border-radius: 2px;
  animation: confettiFall linear infinite;
}

@keyframes confettiFall {
  0% {
    transform: translateY(0) rotate(0deg);
    opacity: 1;
  }
  100% {
    transform: translateY(100vh) rotate(720deg);
    opacity: 0;
  }
}

.result-content {
  position: relative;
  z-index: 2;
  max-width: 440px;
  width: 100%;
}

/* 证书 */
.certificate {
  margin-bottom: 32px;
}

.cert-border {
  background: linear-gradient(135deg, #c9a84c, #8b6914, #c9a84c, #e8c864, #c9a84c);
  padding: 3px;
  border-radius: 16px;
  box-shadow: 0 0 40px rgba(201, 168, 76, 0.3);
}

.cert-inner {
  background: linear-gradient(180deg, #1a0a2e, #0d0518);
  border-radius: 14px;
  padding: 36px 28px;
  text-align: center;
  position: relative;
}

.cert-trophy {
  font-size: 3.5rem;
  margin-bottom: 12px;
  animation: bounce 1s ease infinite;
}

@keyframes bounce {
  0%, 100% { transform: translateY(0); }
  50% { transform: translateY(-8px); }
}

.cert-title {
  font-size: 2rem;
  color: #c9a84c;
  text-shadow: 0 0 16px rgba(201, 168, 76, 0.5);
  letter-spacing: 6px;
  margin-bottom: 16px;
}

.cert-divider {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 20px;
}

.divider-line {
  flex: 1;
  height: 1px;
  background: linear-gradient(90deg, transparent, #c9a84c, transparent);
}

.divider-gem {
  font-size: 1.2rem;
}

.cert-text {
  font-size: 0.9rem;
  color: #b8b0a0;
  line-height: 1.8;
  margin-bottom: 8px;
}

.cert-meta {
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid rgba(201, 168, 76, 0.2);
}

.cert-date {
  font-size: 0.9rem;
  color: #c9a84c;
}

.cert-time {
  font-size: 0.8rem;
  color: #6a5a80;
  margin-top: 4px;
}

/* 水印 */
.watermark {
  margin-top: 24px;
  padding: 12px;
  font-size: 0.75rem;
  color: rgba(201, 168, 76, 0.3);
  border: 1px dashed rgba(201, 168, 76, 0.15);
  border-radius: 6px;
  letter-spacing: 1px;
}

/* 按钮 */
.action-buttons {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.wechat-btn {
  width: 100%;
  padding: 14px;
  font-size: 1rem;
  font-weight: bold;
  color: #1a0a2e;
  background: linear-gradient(135deg, #c9a84c, #e8c864);
  border: none;
  border-radius: 12px;
  cursor: pointer;
  letter-spacing: 2px;
  transition: all 0.2s ease;
  box-shadow: 0 0 20px rgba(201, 168, 76, 0.3);
}

.wechat-btn:hover {
  box-shadow: 0 0 30px rgba(201, 168, 76, 0.5);
}

.restart-btn {
  width: 100%;
  padding: 14px;
  font-size: 1rem;
  font-weight: bold;
  color: #c9a84c;
  background: transparent;
  border: 1px solid rgba(201, 168, 76, 0.4);
  border-radius: 12px;
  cursor: pointer;
  letter-spacing: 2px;
  transition: all 0.2s ease;
}

.restart-btn:hover {
  background: rgba(201, 168, 76, 0.1);
  border-color: #c9a84c;
}

/* 微信弹窗 */
.modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.7);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 100;
  padding: 20px;
}

.wechat-modal {
  background: linear-gradient(180deg, #2a1050, #1a0a2e);
  border: 1px solid rgba(201, 168, 76, 0.3);
  border-radius: 16px;
  padding: 32px 24px;
  max-width: 360px;
  width: 100%;
  text-align: center;
}

.wechat-title {
  font-size: 1.3rem;
  color: #c9a84c;
  margin-bottom: 8px;
}

.wechat-subtitle {
  font-size: 0.85rem;
  color: #a090b0;
  margin-bottom: 24px;
}

.wechat-id-box {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 14px;
  background: rgba(201, 168, 76, 0.08);
  border: 1px solid rgba(201, 168, 76, 0.3);
  border-radius: 10px;
  margin-bottom: 20px;
}

.wechat-label {
  font-size: 0.8rem;
  color: #6a5a80;
}

.wechat-id {
  font-size: 1.1rem;
  color: #e0d8c8;
  font-weight: bold;
  letter-spacing: 1px;
}

.copy-btn {
  width: 100%;
  padding: 12px;
  font-size: 0.95rem;
  font-weight: bold;
  color: #1a0a2e;
  background: linear-gradient(135deg, #c9a84c, #e8c864);
  border: none;
  border-radius: 10px;
  cursor: pointer;
  margin-bottom: 12px;
  transition: all 0.2s ease;
}

.copy-btn:hover {
  box-shadow: 0 0 16px rgba(201, 168, 76, 0.4);
}

.close-btn {
  width: 100%;
  padding: 10px;
  font-size: 0.9rem;
  color: #6a5a80;
  background: transparent;
  border: 1px solid rgba(255,255,255,0.1);
  border-radius: 10px;
  cursor: pointer;
}

.close-btn:hover {
  color: #a090b0;
}

/* 过渡 */
.modal-enter-active,
.modal-leave-active {
  transition: opacity 0.3s ease;
}

.modal-enter-from,
.modal-leave-to {
  opacity: 0;
}
</style>