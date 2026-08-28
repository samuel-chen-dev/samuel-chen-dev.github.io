<template>
  <div class="game">
    <!-- 顶部栏 -->
    <header class="game-header">
      <div class="level-info">
        <span class="level-badge">第 {{ levelData?.level_id || 1 }} 关</span>
        <span class="level-title">{{ levelData?.title || '加载中...' }}</span>
      </div>
      <div class="keys-display">
        <span class="key-icon">🔑</span>
        <span class="key-count">× {{ keysCollected }}</span>
      </div>
    </header>

    <!-- 进度条 -->
    <div class="progress-bar">
      <div class="progress-fill" :style="{ width: progressPercent + '%' }"></div>
    </div>

    <!-- 剧情区 -->
    <section v-if="loading" class="status-panel" aria-live="polite">
      <div class="loading-spinner" aria-hidden="true"></div>
      <p>正在打开古堡房间...</p>
    </section>

    <section v-else-if="loadError" class="status-panel error-panel" role="alert">
      <p class="status-title">关卡暂时无法打开</p>
      <p class="status-message">{{ loadError }}</p>
      <button class="continue-btn retry-btn" @click="loadLevel">重新加载</button>
    </section>

    <section v-else-if="!showResult" class="story-section">
      <div class="story-box">
        <p class="story-text">{{ levelData?.story_before }}</p>
      </div>

      <!-- 题目区 -->
      <div class="question-box">
        <p class="question-text">{{ levelData?.question }}</p>
      </div>

      <!-- 选项 -->
      <div class="options-list">
        <button
          v-for="(opt, idx) in levelData?.options || []"
          :key="idx"
          class="option-btn"
          :class="{
            selected: selectedIndex === idx,
            correct: showResult && resultData?.is_correct && idx === selectedIndex,
            wrong: showResult && !resultData?.is_correct && idx === selectedIndex,
          }"
          :disabled="submitting"
          @click="selectOption(idx)"
        >
          <span class="option-letter">{{ ['A', 'B', 'C', 'D'][idx] }}</span>
          <span class="option-text">{{ opt }}</span>
        </button>
      </div>

      <!-- 提交按钮 -->
      <button
        class="submit-btn"
        :disabled="selectedIndex === null || submitting"
        @click="submitAnswer"
      >
        {{ submitting ? '提交中...' : '提交答案' }}
      </button>
      <p v-if="submitError" class="submit-error" role="alert">{{ submitError }}</p>
    </section>

    <!-- 结果弹窗 -->
    <Transition name="modal">
      <div v-if="showResult" class="modal-overlay" @click.self="closeResult">
        <div class="modal-card" :class="{ correct: resultData?.is_correct }">
          <!-- 正确 -->
          <template v-if="resultData?.is_correct">
            <div class="result-icon">🗝️</div>
            <h2 class="result-title">获得钥匙！</h2>
            <p class="result-story">{{ levelData?.story_after_correct }}</p>
            <div class="key-badge">{{ levelData?.key_name }}</div>
          </template>

          <!-- 错误 -->
          <template v-else>
            <div class="result-icon">💔</div>
            <h2 class="result-title wrong-title">回答错误</h2>
            <div class="error-box">
              <p class="error-label">错误类型：{{ resultData?.error_type }}</p>
              <p class="error-explain">{{ resultData?.explanation }}</p>
            </div>

            <!-- AI 跟进题 -->
            <div v-if="resultData?.follow_up && !showFollowUpResult" class="follow-up-section">
              <p class="follow-up-hint">来试试这道同类题吧：</p>
              <p class="follow-up-question">{{ resultData.follow_up.question }}</p>
              <div class="options-list follow-up-options">
                <button
                  v-for="(opt, idx) in resultData.follow_up.options"
                  :key="idx"
                  class="option-btn follow-up-btn"
                  :class="{ selected: followUpSelected === idx }"
                  @click="followUpSelected = idx"
                >
                  <span class="option-letter">{{ ['A', 'B', 'C', 'D'][idx] }}</span>
                  <span class="option-text">{{ opt }}</span>
                </button>
              </div>
              <button
                class="submit-btn follow-up-submit"
                :disabled="followUpSelected === null"
                @click="checkFollowUp"
              >
                确认
              </button>
            </div>

            <!-- 跟进题结果 -->
            <div v-if="showFollowUpResult" class="follow-up-result">
              <p v-if="followUpCorrect" class="fu-correct">正确！做得很好，可以继续前进了～</p>
              <p v-else class="fu-wrong">还是不太对哦，不过没关系，再想想原题吧～</p>
            </div>
          </template>

          <button class="continue-btn" @click="closeResult">
            {{ resultData?.is_correct ? '继续前进' : '再试一次' }}
          </button>
        </div>
      </div>
    </Transition>

    <!-- 通关弹窗 -->
    <Transition name="modal">
      <div v-if="showCompleted" class="modal-overlay">
        <div class="modal-card completed-card">
          <div class="result-icon">🏆</div>
          <h2 class="result-title">恭喜通关！</h2>
          <p class="completed-text">你已集齐所有 {{ keysCollected }} 把钥匙，古堡的诅咒已被打破！</p>
          <button class="continue-btn" @click="goToResult">查看证书</button>
        </div>
      </div>
    </Transition>
  </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue'
import { useRouter } from 'vue-router'
import { useGameStore } from '../store/gameStore'
import { getStateApi, submitAnswerApi } from '../api'

const router = useRouter()
const store = useGameStore()

const levelData = ref(null)
const loading = ref(false)
const loadError = ref('')
const submitError = ref('')
const selectedIndex = ref(null)
const submitting = ref(false)
const showResult = ref(false)
const resultData = ref(null)
const showCompleted = ref(false)
const keysCollected = ref(0)

// AI 跟进题
const followUpSelected = ref(null)
const showFollowUpResult = ref(false)
const followUpCorrect = ref(false)

const progressPercent = computed(() => {
  if (!levelData.value) return 0
  return ((keysCollected.value) / (levelData.value.total_levels || 10)) * 100
})

onMounted(async () => {
  if (!store.user_id) {
    router.push('/')
    return
  }
  await loadLevel()
})

async function loadLevel() {
  loading.value = true
  loadError.value = ''
  try {
    const data = await getStateApi(store.user_id)
    if (data.is_completed) {
      keysCollected.value = data.keys
      showCompleted.value = true
      return
    }
    levelData.value = data
    keysCollected.value = data.keys_collected
  } catch (e) {
    console.error('加载关卡失败:', e)
    loadError.value = e.response?.data?.detail || '请检查网络连接后重试。'
  } finally {
    loading.value = false
  }
}

function selectOption(idx) {
  if (submitting.value) return
  selectedIndex.value = idx
}

async function submitAnswer() {
  if (selectedIndex.value === null || submitting.value) return
  submitting.value = true
  submitError.value = ''
  try {
    const res = await submitAnswerApi(
      store.user_id,
      levelData.value.level_id,
      selectedIndex.value
    )
    resultData.value = res
    keysCollected.value = res.keys_collected

    if (res.is_completed) {
      showResult.value = true
      await store.updateProgress()
      setTimeout(() => {
        showResult.value = false
        showCompleted.value = true
      }, 1500)
    } else {
      showResult.value = true
    }
  } catch (e) {
    console.error('提交失败:', e)
    submitError.value = e.response?.data?.detail || '答案提交失败，请稍后再试。'
  } finally {
    submitting.value = false
  }
}

function checkFollowUp() {
  if (followUpSelected.value === null) return
  const correct = resultData.value.follow_up.answer_index
  followUpCorrect.value = followUpSelected.value === correct
  showFollowUpResult.value = true
}

function closeResult() {
  if (resultData.value?.is_correct || showFollowUpResult.value) {
    showResult.value = false
    showFollowUpResult.value = false
    followUpSelected.value = null
    selectedIndex.value = null
    resultData.value = null
    loadLevel()
  } else {
    showResult.value = false
    selectedIndex.value = null
  }
}

function goToResult() {
  router.push('/result')
}
</script>

<style scoped>
.game {
  min-height: 100vh;
  background: radial-gradient(ellipse at top, #2a1050 0%, #1a0a2e 50%, #0d0518 100%);
  padding-bottom: 40px;
}

.status-panel {
  width: min(520px, calc(100% - 40px));
  margin: 90px auto 0;
  padding: 36px 24px;
  text-align: center;
  color: #d0c8b8;
  background: rgba(26, 10, 46, 0.78);
  border: 1px solid rgba(201, 168, 76, 0.25);
  border-radius: 12px;
}

.loading-spinner {
  width: 28px;
  height: 28px;
  margin: 0 auto 14px;
  border: 3px solid rgba(201, 168, 76, 0.25);
  border-top-color: #c9a84c;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.error-panel { border-color: rgba(255, 107, 107, 0.4); }
.status-title { color: #f0e8d8; font-weight: bold; margin-bottom: 8px; }
.status-message, .submit-error { color: #ffb3b3; font-size: 0.9rem; }
.retry-btn { margin-top: 20px; }
.submit-error { margin-top: -14px; margin-bottom: 16px; text-align: center; }

/* 顶部栏 */
.game-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  background: rgba(26, 10, 46, 0.9);
  border-bottom: 1px solid rgba(201, 168, 76, 0.2);
  position: sticky;
  top: 0;
  z-index: 10;
}

.level-info {
  display: flex;
  align-items: center;
  gap: 10px;
}

.level-badge {
  background: #c9a84c;
  color: #1a0a2e;
  padding: 4px 12px;
  border-radius: 20px;
  font-size: 0.8rem;
  font-weight: bold;
}

.level-title {
  font-size: 1rem;
  color: #e0d8c8;
  font-weight: bold;
}

.keys-display {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 1.1rem;
  color: #c9a84c;
}

.key-icon {
  font-size: 1.3rem;
}

/* 进度条 */
.progress-bar {
  height: 4px;
  background: rgba(201, 168, 76, 0.15);
}

.progress-fill {
  height: 100%;
  background: linear-gradient(90deg, #c9a84c, #e8c864);
  transition: width 0.5s ease;
}

/* 剧情 */
.story-section {
  padding: 20px;
}

.story-box {
  background: rgba(26, 10, 46, 0.7);
  border: 1px solid rgba(201, 168, 76, 0.2);
  border-radius: 12px;
  padding: 20px;
  margin-bottom: 20px;
}

.story-text {
  font-size: 0.95rem;
  line-height: 1.8;
  color: #c8c0b0;
}

/* 题目 */
.question-box {
  background: rgba(201, 168, 76, 0.08);
  border: 1px solid rgba(201, 168, 76, 0.3);
  border-radius: 12px;
  padding: 20px;
  margin-bottom: 20px;
}

.question-text {
  font-size: 1.1rem;
  color: #f0e8d8;
  font-weight: bold;
  letter-spacing: 0.5px;
}

/* 选项 */
.options-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-bottom: 24px;
}

.option-btn {
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 14px 16px;
  background: rgba(26, 10, 46, 0.7);
  border: 1px solid rgba(201, 168, 76, 0.2);
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.2s ease;
  color: #d0c8b8;
  font-size: 0.95rem;
  text-align: left;
}

.option-btn:hover:not(:disabled) {
  border-color: rgba(201, 168, 76, 0.5);
  background: rgba(201, 168, 76, 0.08);
}

.option-btn.selected {
  border-color: #c9a84c;
  background: rgba(201, 168, 76, 0.15);
  box-shadow: 0 0 12px rgba(201, 168, 76, 0.2);
}

.option-btn.correct {
  border-color: #4caf50;
  background: rgba(76, 175, 80, 0.15);
}

.option-btn.wrong {
  border-color: #8b0000;
  background: rgba(139, 0, 0, 0.15);
}

.option-btn:disabled {
  opacity: 0.7;
  cursor: not-allowed;
}

.option-letter {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 28px;
  height: 28px;
  border-radius: 50%;
  background: rgba(201, 168, 76, 0.2);
  color: #c9a84c;
  font-weight: bold;
  font-size: 0.85rem;
  flex-shrink: 0;
}

.option-btn.selected .option-letter {
  background: #c9a84c;
  color: #1a0a2e;
}

/* 提交按钮 */
.submit-btn {
  width: 100%;
  padding: 14px;
  font-size: 1.05rem;
  font-weight: bold;
  color: #1a0a2e;
  background: linear-gradient(135deg, #c9a84c, #e8c864);
  border: none;
  border-radius: 12px;
  cursor: pointer;
  letter-spacing: 2px;
  transition: all 0.2s ease;
}

.submit-btn:hover:not(:disabled) {
  box-shadow: 0 0 20px rgba(201, 168, 76, 0.4);
}

.submit-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

/* 弹窗 */
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

.modal-card {
  background: linear-gradient(180deg, #2a1050, #1a0a2e);
  border: 1px solid rgba(201, 168, 76, 0.3);
  border-radius: 16px;
  padding: 32px 24px;
  max-width: 400px;
  width: 100%;
  text-align: center;
  max-height: 85vh;
  overflow-y: auto;
}

.modal-card.correct {
  border-color: rgba(76, 175, 80, 0.4);
}

.result-icon {
  font-size: 3rem;
  margin-bottom: 12px;
}

.result-title {
  font-size: 1.5rem;
  color: #c9a84c;
  margin-bottom: 12px;
}

.wrong-title {
  color: #8b0000;
}

.result-story {
  font-size: 0.9rem;
  color: #c8c0b0;
  line-height: 1.7;
  margin-bottom: 16px;
}

.key-badge {
  display: inline-block;
  padding: 8px 20px;
  background: rgba(201, 168, 76, 0.15);
  border: 1px solid #c9a84c;
  border-radius: 20px;
  color: #c9a84c;
  font-size: 0.9rem;
  margin-bottom: 16px;
}

.error-box {
  background: rgba(139, 0, 0, 0.1);
  border: 1px solid rgba(139, 0, 0, 0.3);
  border-radius: 10px;
  padding: 16px;
  margin-bottom: 16px;
  text-align: left;
}

.error-label {
  font-size: 0.85rem;
  color: #ff6b6b;
  margin-bottom: 8px;
  font-weight: bold;
}

.error-explain {
  font-size: 0.9rem;
  color: #d0c8b8;
  line-height: 1.6;
}

/* 跟进题 */
.follow-up-section {
  margin-top: 8px;
  text-align: left;
}

.follow-up-hint {
  font-size: 0.85rem;
  color: #c9a84c;
  margin-bottom: 8px;
}

.follow-up-question {
  font-size: 0.95rem;
  color: #f0e8d8;
  font-weight: bold;
  margin-bottom: 12px;
  line-height: 1.6;
}

.follow-up-options {
  margin-bottom: 12px;
}

.follow-up-btn {
  padding: 10px 14px;
  font-size: 0.85rem;
}

.follow-up-submit {
  font-size: 0.9rem;
  padding: 10px;
}

.follow-up-result {
  margin-top: 8px;
  padding: 12px;
  border-radius: 8px;
}

.fu-correct {
  color: #4caf50;
  font-weight: bold;
}

.fu-wrong {
  color: #ff6b6b;
}

.continue-btn {
  width: 100%;
  padding: 12px;
  font-size: 1rem;
  font-weight: bold;
  color: #1a0a2e;
  background: linear-gradient(135deg, #c9a84c, #e8c864);
  border: none;
  border-radius: 10px;
  cursor: pointer;
  margin-top: 12px;
  transition: all 0.2s ease;
}

.continue-btn:hover {
  box-shadow: 0 0 16px rgba(201, 168, 76, 0.4);
}

/* 通关卡片 */
.completed-card {
  border-color: #c9a84c;
}

.completed-text {
  font-size: 0.95rem;
  color: #c8c0b0;
  line-height: 1.7;
  margin-bottom: 16px;
}

/* 过渡动画 */
.modal-enter-active,
.modal-leave-active {
  transition: opacity 0.3s ease;
}

.modal-enter-from,
.modal-leave-to {
  opacity: 0;
}
</style>