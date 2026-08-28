<template>
  <div class="home">
    <!-- 背景装饰 -->
    <div class="bg-overlay"></div>
    <div class="bg-particles">
      <span v-for="i in 20" :key="i" class="particle" :style="particleStyle(i)"></span>
    </div>

    <div class="home-content">
      <!-- 标题 -->
      <div class="title-section">
        <h1 class="main-title">失落翡翠之眼</h1>
        <p class="subtitle">古堡探险英语闯关</p>
      </div>

      <!-- 故事卡片 -->
      <div class="story-card">
        <p class="story-text">
          在迷雾笼罩的古老城堡深处，传说藏着一颗拥有神秘力量的翡翠之眼。
          但要获得它，你必须通过十道英语语法的考验，解开古堡的层层封印。
        </p>
        <p class="story-text">
          勇敢的探险家，你准备好了吗？
        </p>
      </div>

      <!-- 开始按钮 -->
      <button class="start-btn" :disabled="starting" @click="startGame">
        <span class="btn-icon"></span>
        {{ starting ? '正在准备...' : '开始探险' }}
        <span class="btn-icon"></span>
      </button>
      <p v-if="startError" class="start-error" role="alert">{{ startError }}</p>

      <!-- 社交证明 -->
      <p class="social-proof">已有 128 人成功逃脱</p>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useGameStore } from '../store/gameStore'

const router = useRouter()
const store = useGameStore()
const starting = ref(false)
const startError = ref('')

function particleStyle(i) {
  const size = Math.random() * 4 + 2
  return {
    left: `${Math.random() * 100}%`,
    top: `${Math.random() * 100}%`,
    width: `${size}px`,
    height: `${size}px`,
    animationDelay: `${Math.random() * 5}s`,
    animationDuration: `${Math.random() * 3 + 3}s`,
  }
}

async function startGame() {
  if (starting.value) return
  starting.value = true
  startError.value = ''
  // 生成一个简易 openid（实际项目中由微信登录获取）
  if (!store.openid) {
    store.openid = 'guest_' + Date.now() + '_' + Math.random().toString(36).slice(2, 8)
  }
  try {
    await store.login()
    router.push('/game')
  } catch (e) {
    console.error('登录失败:', e)
    startError.value = e.response?.data?.detail || '暂时无法连接游戏服务，请稍后重试。'
  } finally {
    starting.value = false
  }
}
</script>

<style scoped>
.home {
  position: relative;
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  background: radial-gradient(ellipse at center, #2a1050 0%, #1a0a2e 60%, #0d0518 100%);
}

.bg-overlay {
  position: absolute;
  inset: 0;
  background: url('data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><rect width="100" height="100" fill="none" stroke="%23c9a84c" stroke-width="0.3" opacity="0.1"/></svg>');
  opacity: 0.3;
}

.particle {
  position: absolute;
  background: #c9a84c;
  border-radius: 50%;
  animation: float 4s ease-in-out infinite;
  opacity: 0.4;
}

@keyframes float {
  0%, 100% { transform: translateY(0); opacity: 0.2; }
  50% { transform: translateY(-20px); opacity: 0.6; }
}

.home-content {
  position: relative;
  z-index: 1;
  text-align: center;
  padding: 40px 24px;
  max-width: 480px;
  width: 100%;
}

.title-section {
  margin-bottom: 32px;
}

.main-title {
  font-size: 2.4rem;
  color: #c9a84c;
  text-shadow: 0 0 20px rgba(201, 168, 76, 0.5), 0 2px 4px rgba(0,0,0,0.5);
  letter-spacing: 4px;
  margin-bottom: 8px;
}

.subtitle {
  font-size: 1rem;
  color: #a090b0;
  letter-spacing: 6px;
}

.story-card {
  background: rgba(26, 10, 46, 0.7);
  border: 1px solid rgba(201, 168, 76, 0.3);
  border-radius: 12px;
  padding: 24px 20px;
  margin-bottom: 36px;
  backdrop-filter: blur(4px);
}

.story-text {
  font-size: 0.95rem;
  line-height: 1.8;
  color: #d0c8b8;
  margin-bottom: 8px;
}

.story-text:last-child {
  margin-bottom: 0;
  color: #c9a84c;
  font-weight: bold;
}

.start-btn {
  display: inline-flex;
  align-items: center;
  gap: 12px;
  padding: 16px 48px;
  font-size: 1.2rem;
  font-weight: bold;
  color: #1a0a2e;
  background: linear-gradient(135deg, #c9a84c, #e8c864);
  border: none;
  border-radius: 50px;
  cursor: pointer;
  letter-spacing: 2px;
  transition: all 0.3s ease;
  box-shadow: 0 0 30px rgba(201, 168, 76, 0.4);
}

.start-btn:hover {
  transform: scale(1.05);
  box-shadow: 0 0 40px rgba(201, 168, 76, 0.6);
}

.start-btn:active {
  transform: scale(0.98);
}

.start-btn:disabled {
  cursor: wait;
  opacity: 0.7;
}

.start-error {
  margin-top: 16px;
  color: #ffb3b3;
  font-size: 0.85rem;
}

.btn-icon {
  display: inline-block;
  width: 8px;
  height: 8px;
  background: #1a0a2e;
  border-radius: 50%;
  opacity: 0.5;
}

.social-proof {
  margin-top: 24px;
  font-size: 0.8rem;
  color: #6a5a80;
  letter-spacing: 1px;
}
</style>