const LEVELS = [
  {
    topic: '性格与关系',
    rows: [
      ['Jenny talks to everyone and enjoys making new friends. She is ___.', ['outgoing', 'silent', 'careless', 'nervous'], 0, '性格形容词辨析', 'outgoing表示外向且乐于交际，符合主动结交朋友的描述', 'silent、careless和nervous分别表示沉默、粗心和紧张，与语境不符'],
      ['A good friend should be ___ and ready to help when you are in trouble.', ['serious', 'caring', 'famous', 'common'], 1, '人物品质词汇', 'caring表示关心他人，能与ready to help形成意义呼应', 'serious、famous和common不直接表示乐于助人的品质'],
      ['Leo is ___ than his brother; he checks every answer twice.', ['more popular', 'more creative', 'more careful', 'more humorous'], 2, '性格词与行为线索', '检查两遍体现做事更仔细，因此more careful最准确', 'popular、creative和humorous都不能由检查答案这一行为推出'],
      ['Although Emma and I have different hobbies, we ___ well because we respect each other.', ['look after', 'find out', 'grow up', 'get along'], 3, '人际关系动词短语', 'get along well表示相处融洽，尊重彼此解释了良好关系', 'look after、find out和grow up分别表示照顾、查明和长大'],
      ['Tom is not as ___ as he seems; he often makes everyone laugh after class.', ['serious', 'friendly', 'patient', 'honest'], 0, '否定比较与性格推断', 'not as serious as he seems表示他并不像外表那样严肃，后句的逗笑行为提供证据', 'friendly、patient和honest与“看起来”和逗笑之间没有反差关系'],
    ],
  },
  {
    topic: '媒体与娱乐',
    rows: [
      ['The evening ___ reported that a new bridge would open next week.', ['cartoon', 'news', 'comedy', 'opera'], 1, '媒体节目类别', 'news负责报道新桥开放这样的现实事件', 'cartoon、comedy和opera属于娱乐或艺术形式，不承担新闻报道功能'],
      ['The host asked the actor several questions during the live ___.', ['menu', 'ticket', 'interview', 'screen'], 2, '媒体活动名词', 'interview指主持人与嘉宾之间的采访，符合提问场景', 'menu、ticket和screen分别是菜单、票和屏幕，不能承接asked questions'],
      ['This nature ___ shows how pandas live in the wild.', ['sitcom', 'talent show', 'talk show', 'documentary'], 3, '节目类型辨析', 'documentary是记录真实自然与社会内容的纪录片', 'sitcom、talent show和talk show分别侧重喜剧、才艺和访谈'],
      ['The film received good ___ because its story was moving and believable.', ['reviews', 'reasons', 'results', 'records'], 0, '影视评价词汇', 'reviews表示观众或评论家的影评，good reviews是常见搭配', 'reasons、results和records不表示对电影质量的公开评价'],
      ['The program is meant to ___ teenagers, but it also makes them think about friendship.', ['report', 'entertain', 'publish', 'search'], 1, '媒体功能动词', 'entertain表示给观众带来娱乐，与program的主要功能一致', 'report、publish和search的宾语及使用场景均与句意不合'],
    ],
  },
  {
    topic: '职业与规划',
    rows: [
      ['A ___ designs buildings and makes sure they are safe.', ['pilot', 'dentist', 'engineer', 'cook'], 2, '职业职责识别', 'engineer可负责建筑设计和安全技术工作', 'pilot、dentist和cook分别从事飞行、牙科和烹饪工作'],
      ['Mia wants to be a writer, so she plans to ___ writing every day.', ['give up', 'turn down', 'take away', 'practice'], 3, '职业计划动词', 'practice writing表示通过每日练习为写作职业做准备', 'give up、turn down和take away均不能表达持续提升技能'],
      ['My brother is going to ___ computer science at university.', ['study', 'teach', 'solve', 'build'], 0, '专业学习搭配', 'study computer science表示在大学学习计算机科学专业', 'teach需要教学身份，solve和build不能直接与学科名构成该搭配'],
      ['To become a musician, Nina decided to ___ the piano seriously.', ['look for', 'take up', 'care about', 'hear from'], 1, '计划类动词短语', 'take up表示开始从事一项爱好或活动，符合开始认真学钢琴', 'look for、care about和hear from意义分别为寻找、关心和收到来信'],
      ['A dream will remain only a dream unless you make a practical ___ to achieve it.', ['promise', 'example', 'plan', 'difference'], 2, '目标规划抽象名词', 'make a practical plan表示制定可执行计划，与achieve呼应', 'promise、example和difference不能准确表达实现目标的步骤安排'],
    ],
  },
  {
    topic: '科技与未来城市',
    rows: [
      ['A smart ___ can turn the lights off when nobody is in the room.', ['planet', 'factory', 'environment', 'device'], 3, '科技产品名词', 'device指执行特定功能的设备，可自动控制灯光', 'planet、factory和environment都不是房间内的小型智能设备'],
      ['More electric buses may help reduce air ___ in large cities.', ['pollution', 'prediction', 'population', 'production'], 0, '环保科技词汇', 'electric buses减少尾气，从而降低air pollution', 'prediction、population和production虽形近但不表示空气污染'],
      ['The robot can ___ simple tasks such as carrying boxes.', ['appear', 'perform', 'promise', 'prepare'], 1, '科技动词搭配', 'perform tasks是执行任务的固定搭配', 'appear、promise和prepare不能直接表达机器人完成任务'],
      ['Scientists are trying to develop cleaner forms of ___ for future cities.', ['information', 'education', 'energy', 'temperature'], 2, '未来城市主题词', 'clean energy指太阳能等清洁能源，符合城市可持续发展', 'information、education和temperature都不是可被开发的能源形式'],
      ['The app does not replace doctors; instead, it ___ them in checking health data.', ['controls', 'invents', 'refuses', 'assists'], 3, '科技作用精准表达', 'assist somebody in doing表示协助某人做某事，准确说明应用的辅助角色', 'control、invent和refuse会夸大、改变或否定应用与医生的关系'],
    ],
  },
  {
    topic: '烹饪与数量',
    rows: [
      ['Please ___ the bananas before putting them into the bowl.', ['peel', 'pour', 'boil', 'serve'], 0, '烹饪动作识别', '香蕉放入碗前要先peel，即剥皮', 'pour、boil和serve分别表示倒、煮沸和上菜'],
      ['Add two ___ of honey to make the drink sweeter.', ['plates', 'spoons', 'bags', 'pairs'], 1, '食材量词搭配', 'honey常用spoons衡量，two spoons of honey搭配自然', 'plates、bags和pairs不适合少量蜂蜜'],
      ['After cutting the vegetables, ___ them together in a large bowl.', ['cover', 'fill', 'mix', 'dig'], 2, '步骤动词辨析', 'mix表示把多种蔬菜混合在一起', 'cover、fill和dig分别表示覆盖、装满和挖，不符合together的提示'],
      ['Could you ___ the glass with warm water, but not to the top?', ['add', 'pour', 'shake', 'fill'], 3, '容器动词搭配', 'fill the glass with water是“用水装杯子”的固定结构', 'add和pour通常以液体作宾语，shake不表示装入'],
      ['There is ___ salt in the soup, so it tastes too salty.', ['too much', 'too many', 'a few', 'a number of'], 0, '可数与不可数数量表达', 'salt是不可数名词，表示过量应用too much', 'too many、a few和a number of都修饰可数名词复数'],
    ],
  },
  {
    topic: '邀请与日程',
    rows: [
      ['Are you ___ this Saturday? We are planning a picnic.', ['careful', 'available', 'successful', 'comfortable'], 1, '日程状态形容词', 'available表示有空，可以参加活动', 'careful、successful和comfortable都不用于询问时间是否空闲'],
      ['Ben had to ___ the invitation because he had a piano lesson.', ['accept', 'prepare', 'refuse', 'celebrate'], 2, '邀请回应动词', '因课程冲突而不能参加，应refuse邀请', 'accept表示接受，prepare和celebrate不能表达拒绝'],
      ['Let me check my ___ before I promise to join you.', ['reason', 'message', 'calendar', 'schedule'], 3, '日程词义精度', 'check my schedule表示查看已安排的具体日程', 'calendar偏日期载体，reason和message不记录时间安排'],
      ['Thank you for your ___. I would love to come to the party.', ['invitation', 'information', 'attention', 'competition'], 0, '社交名词搭配', 'thank somebody for the invitation用于感谢邀请', 'information、attention和competition均不能与参加派对自然呼应'],
      ['The meeting was ___ until Friday because two members were ill.', ['taken up', 'put off', 'turned on', 'given away'], 1, '日程变更短语', 'put off表示将会议推迟到周五', 'take up、turn on和give away分别表示占用、打开和赠送'],
    ],
  },
  {
    topic: '疾病与安全',
    rows: [
      ['If you have a high ___, you should rest and drink water.', ['cough', 'pain', 'fever', 'wound'], 2, '常见症状词汇', 'a high fever表示发高烧，是常见固定搭配', 'cough、pain和wound不能用high直接描述体温'],
      ['The boy fell off his bike and hurt his ___.', ['medicine', 'health', 'danger', 'knee'], 3, '事故与身体部位', '跌下自行车容易伤到knee，hurt one’s knee搭配自然', 'medicine、health和danger不是可直接受伤的身体部位'],
      ['Wash the small cut and cover it with a clean ___.', ['bandage', 'temperature', 'toothache', 'accident'], 0, '基础急救用品', '清洗伤口后用clean bandage包扎是基础处理', 'temperature、toothache和accident都不是包扎用品'],
      ['You should not move an injured person unless there is immediate ___.', ['service', 'danger', 'training', 'exercise'], 1, '急救安全判断', '只有现场存在immediate danger时才应紧急移动伤者', 'service、training和exercise不能构成必须移动伤者的紧迫原因'],
      ['Wearing a helmet can ___ the risk of a serious head injury.', ['raise', 'cause', 'reduce', 'spread'], 2, '安全风险动词', '头盔能够reduce the risk，即降低严重头部受伤风险', 'raise、cause和spread分别表示提高、造成和传播，与防护作用相反'],
    ],
  },
  {
    topic: '志愿服务',
    rows: [
      ['The students worked as ___ at the city library on Sunday.', ['leaders', 'visitors', 'readers', 'volunteers'], 3, '志愿服务身份词', 'volunteers指无偿帮助图书馆工作的志愿者', 'leaders、visitors和readers不能体现主动服务'],
      ['Our class plans to ___ money for children in need.', ['raise', 'borrow', 'waste', 'count'], 0, '公益筹款搭配', 'raise money表示为公益目的筹集资金', 'borrow、waste和count分别表示借、浪费和计数'],
      ['The community center ___ free meals for older people every Friday.', ['offers to', 'provides', 'borrows', 'carries'], 1, '公益供给动词', 'provide something for somebody表示为老人提供免费餐食', 'offers to后应接动词，borrows和carries不表达公益供给'],
      ['Helping at the animal center gave Mia a strong sense of ___.', ['difficulty', 'difference', 'satisfaction', 'discussion'], 2, '志愿服务感受词', 'a sense of satisfaction表示帮助他人后的满足感', 'difficulty、difference和discussion不能表达内在成就体验'],
      ['Instead of giving away answers, the volunteer ___ the child to solve the problem alone.', ['ordered', 'allowed', 'forced', 'encouraged'], 3, '帮助方式与语境', 'encourage somebody to do表示鼓励孩子独立解决问题，符合有效帮助', 'order、allow和force的语气或意义不能体现耐心引导'],
    ],
  },
  {
    topic: '高频易混动词',
    rows: [
      ['May I ___ your dictionary for a minute?', ['borrow', 'lend', 'keep', 'carry'], 0, 'borrow与lend辨析', '说话者从对方处借入字典，应使用borrow', 'lend表示借出，keep强调保留一段时间，carry表示携带'],
      ['Could you ___ me your umbrella until tomorrow?', ['borrow', 'lend', 'take', 'bring'], 1, 'lend双宾语结构', 'lend me your umbrella表示把伞借给我', 'borrow方向相反，take和bring表示拿走或带来而非借出'],
      ['You can ___ the novel for two weeks, but return it on time.', ['borrow', 'lend', 'keep', 'bring'], 2, '延续性动词keep', 'for two weeks是一段时间，要用延续性动词keep', 'borrow和lend是瞬间动作，bring不表示借阅期限'],
      ['Please ___ your sports shoes here tomorrow; we will need them for PE.', ['carry', 'take', 'fetch', 'bring'], 3, 'bring与take方向辨析', '从别处把鞋带到说话地点应用bring', 'take表示带离，carry强调搬运，fetch强调去取再回来'],
      ['The box is too heavy to ___ by hand, so we should use a cart to ___ it downstairs.', ['carry; take', 'bring; carry', 'take; bring', 'fetch; lend'], 0, '多组方向动词综合', 'carry强调手提重物，take表示把它从此处带到楼下', '其他组合混淆搬运方式与方向，或出现完全不相关的lend'],
    ],
  },
];

export const GRADE8_VOCABULARY_QUESTIONS = LEVELS.map((level, levelIndex) => ({
  level: levelIndex + 1,
  passScore: 3,
  questions: level.rows.map(([question, options, correctIndex, point, why, wrong], questionIndex) => ({
    id: `grade8_vocabulary_${levelIndex + 1}_${questionIndex + 1}`,
    question,
    options,
    correctIndex,
    explanation: `本题考查${point}。${why}；${wrong}。`,
    type: 'choice',
  })),
}));
