import axios from 'axios'

const api = axios.create({
  baseURL: 'http://localhost:8000/api',
  timeout: 10000,
})

// 请求拦截器
api.interceptors.request.use(
  (config) => config,
  (error) => Promise.reject(error)
)

// 响应拦截器：统一提取 data
api.interceptors.response.use(
  (response) => {
    const { code, data } = response.data
    if (code === 0) return data
    return Promise.reject(new Error('请求失败'))
  },
  (error) => Promise.reject(error)
)

/** 用户登录 */
export function loginApi(openid) {
  return api.post('/auth/login', { openid })
}

/** 获取游戏状态 */
export function getStateApi(user_id) {
  return api.get('/game/state', { params: { user_id } })
}

/** 提交答案 */
export function submitAnswerApi(user_id, level_id, selected_index) {
  return api.post('/game/submit', { user_id, level_id, selected_index })
}

export default api