import { defineStore } from 'pinia'
import { loginApi, getStateApi } from '../api'

const STORAGE_KEY = 'escape_game_user'

export const useGameStore = defineStore('game', {
  state: () => ({
    user_id: null,
    openid: localStorage.getItem(STORAGE_KEY) || '',
    current_level: 0,
    keys: 0,
    is_completed: false,
  }),

  getters: {
    isLoggedIn: (state) => !!state.user_id,
  },

  actions: {
    /** 登录：根据 openid 获取或创建用户 */
    async login() {
      if (!this.openid) {
        throw new Error('openid 不能为空')
      }
      const res = await loginApi(this.openid)
      this.user_id = res.user_id
      this.current_level = res.progress.level
      this.keys = res.progress.keys
      this.is_completed = res.progress.is_completed
      localStorage.setItem(STORAGE_KEY, this.openid)
    },

    /** 刷新游戏进度 */
    async updateProgress() {
      if (!this.user_id) return
      const res = await getStateApi(this.user_id)
      if (res.is_completed) {
        this.is_completed = true
        this.keys = res.keys
      } else {
        this.keys = res.keys_collected
      }
    },

    /** 重置游戏 */
    resetGame() {
      this.user_id = null
      this.openid = ''
      this.current_level = 0
      this.keys = 0
      this.is_completed = false
      localStorage.removeItem(STORAGE_KEY)
    },
  },
})