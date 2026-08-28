const LEVELS = [
  {
    passage: 'Class 8A surveyed 40 students about exercise. Twelve students exercised every day, eighteen exercised three or four times a week, and ten exercised less than twice a week. Running was the most popular activity because it needed no special place. Basketball came second, while swimming was chosen by only five students. Most daily exercisers said they slept better and felt more active in class. The class decided to open a twenty-minute morning exercise club so that busy students could join before lessons.',
    rows: [
      ['How many students exercised every day?', ['Twelve students', 'Eighteen students', 'Ten students', 'Five students'], 0, '细节题', '调查明确指出12名学生每天锻炼'],
      ['Why was running the most popular?', ['It had prizes', 'It needed no special place', 'It was a class rule', 'It was taught daily'], 1, '细节题', '文章说明跑步不需要特殊场地'],
      ['Who will probably benefit most from the morning club?', ['Swimming coaches', 'Daily exercisers only', 'Students with little free time', 'Teachers after work'], 2, '推理题', '俱乐部设在课前是为了方便平时忙碌的学生'],
      ['What is the passage mainly about?', ['A basketball match', 'Ways to sleep', 'A school timetable', 'A survey and its follow-up plan'], 3, '主旨题', '全文先报告运动调查，再说明班级据此采取的行动'],
      ['The word “active” is closest in meaning to ___.', ['full of energy', 'quiet and shy', 'easy to forget', 'slow to respond'], 0, '词义猜测题', '睡得更好后课堂状态改善，active在此表示精力充沛'],
    ],
  },
  {
    passage: 'Online learning gives students access to lessons at any time. Videos can be paused, and learners can review difficult parts. It also saves travel time. However, studying through a screen requires self-control. Messages and games may distract learners, and students cannot always ask a teacher a question immediately. At Green School, teachers now combine online materials with classroom discussion. Students watch short videos at home and solve problems together in class. The school believes technology works best when it supports, rather than replaces, communication.',
    rows: [
      ['What can learners do with lesson videos?', ['Print every video', 'Pause and review them', 'Change the teacher', 'Remove hard parts'], 1, '细节题', '首段明确说明视频可以暂停并复习难点'],
      ['What may distract online learners?', ['Books and notes', 'Teachers and parents', 'Messages and games', 'Questions and answers'], 2, '细节题', '文章直接列出消息和游戏会分散注意力'],
      ['Why does Green School use classroom discussion?', ['To sell devices', 'To shorten holidays', 'To avoid homework', 'To keep human communication'], 3, '推理题', '学校强调技术应支持而非取代交流，因此保留课堂讨论'],
      ['What is the writer’s main point?', ['Online learning has benefits and limits', 'All lessons should move online', 'Technology makes teachers unnecessary', 'Classroom learning wastes travel time'], 0, '主旨题', '文章平衡讨论线上学习的优缺点并给出融合方案'],
      ['The word “access” most nearly means ___.', ['a reason to refuse', 'a chance to use', 'a rule to follow', 'a place to hide'], 1, '词义猜测题', '上下文指学生任何时间都有使用课程的机会'],
    ],
  },
  {
    passage: 'Last winter, Nora volunteered at a community kitchen. On her first day, she expected to serve meals, but the manager asked her to talk with older visitors while they waited. Nora was nervous because she did not know what to say. Then Mr. Chen showed her old photographs of the neighborhood. His stories turned an awkward silence into a warm conversation. Over the next month, Nora learned every visitor’s name and recorded several local stories for the school website. She realized that volunteering was not only about doing tasks; listening could also make people feel valued.',
    rows: [
      ['Where did Nora volunteer?', ['At a school library', 'At an animal center', 'At a community kitchen', 'At a city museum'], 2, '细节题', '首句明确地点是社区厨房'],
      ['What did Mr. Chen show Nora?', ['A recipe book', 'A visitor list', 'A school website', 'Old neighborhood photographs'], 3, '细节题', '文中直接说他展示了社区旧照片'],
      ['How did Nora probably change?', ['She became more confident in listening', 'She stopped serving the community', 'She avoided older visitors', 'She only cared about school work'], 0, '推理题', '从紧张无话到认识所有访客并记录故事，可推断她更会倾听交流'],
      ['What lesson does the story express?', ['Cooking is the best service', 'Listening can be meaningful help', 'Photographs should stay private', 'Websites replace conversations'], 1, '主旨题', '结尾明确点出倾听也能让人感到被重视'],
      ['“Awkward” most nearly describes something ___.', ['exciting and fast', 'simple and clear', 'uncomfortable and difficult', 'valuable and rare'], 2, '词义猜测题', 'Nora不知道说什么造成令人不自在的沉默'],
    ],
  },
  {
    passage: 'Dear School Counselor, I have worked hard this term, but I still worry before every test. I stay up late reviewing because I fear missing a detail. The next day I feel tired and cannot think clearly. My parents tell me to relax, yet that makes me feel they do not understand. Could you suggest a better way? —Leo. Dear Leo, Your effort matters, but sleep is part of learning. Make a weekly plan and divide large tasks into smaller ones. Stop studying thirty minutes before bed and prepare your schoolbag instead. Tell your parents exactly what kind of support you need. If the worry continues to affect daily life, please visit my office. —Ms. Wang',
    rows: [
      ['Why does Leo stay up late?', ['He fears missing details', 'He enjoys quiet nights', 'His parents require it', 'He forgets his schoolbag'], 3, '细节题', 'Leo明确说担心遗漏细节而熬夜复习'],
      ['What should Leo do before bed?', ['Take another test', 'Discuss every detail', 'Study for an extra hour', 'Stop studying and pack his bag'], 0, '细节题', '辅导员建议睡前半小时停止学习并整理书包'],
      ['Why may a weekly plan help?', ['It removes all tests', 'It makes work feel manageable', 'It allows less sleep', 'It replaces family support'], 1, '推理题', '把大任务拆小能降低压力，使任务更可控制'],
      ['What is the main purpose of Ms. Wang’s reply?', ['To criticize Leo’s parents', 'To explain test rules', 'To offer practical stress advice', 'To cancel Leo’s homework'], 2, '主旨题', '回信围绕睡眠、计划、沟通等可执行建议展开'],
      ['The word “affect” is closest in meaning to ___.', ['have an influence on', 'make a promise about', 'pay attention to', 'find an excuse for'], 0, '词义猜测题', '语境指焦虑持续对日常生活产生影响'],
    ],
  },
  {
    passage: 'A violent storm hit Maple Street at dusk. The electricity went out, and rainwater began entering several ground-floor homes. While adults moved furniture upstairs, fourteen-year-old Sam checked on Mrs. Lee, who lived alone. Her phone had no power, so Sam used his bicycle light to guide her to the community hall. Meanwhile, other neighbors shared dry clothes and hot water. By midnight, firefighters had cleared a blocked drain and the water started falling. The storm damaged property, but it also revealed how quickly ordinary neighbors could form a team.',
    rows: [
      ['What happened to the electricity?', ['It became stronger', 'It went out', 'It was sold', 'It heated the water'], 1, '细节题', '文章明确说暴风雨中停电了'],
      ['How did Sam help Mrs. Lee?', ['He fixed her phone', 'He moved her furniture', 'He guided her to safety', 'He cleared the drain'], 2, '细节题', 'Sam用自行车灯引导她前往社区大厅'],
      ['What does Sam’s action show?', ['He wanted a reward', 'He disliked the hall', 'He feared bicycles', 'He noticed a vulnerable neighbor'], 3, '推理题', '他主动查看独居老人并护送，说明关注需要帮助的人'],
      ['What is the central idea?', ['A crisis can bring a community together', 'Storms only damage old buildings', 'Teenagers should fight fires alone', 'Electricity is more useful than water'], 0, '主旨题', '结尾强调灾害显现了邻里迅速协作的力量'],
      ['The word “revealed” means ___.', ['hid completely', 'showed clearly', 'changed slowly', 'questioned openly'], 1, '词义猜测题', '暴风雨把社区协作能力清楚展现出来'],
    ],
  },
  {
    passage: 'In a modern retelling of the story Yu Gong Moves the Mountains, Yu Gong is a student whose village has weak internet service. Instead of carrying stones, he collects connection records and maps places where signals disappear. Some people laugh at his small team, saying the problem is too large. The students continue and present their evidence to a technology company. Engineers finally build a new signal tower. The new version keeps the old story’s spirit: a difficult goal becomes possible through patience. It also suggests that determination today may require data, teamwork and communication.',
    rows: [
      ['What problem does the village have?', ['Weak internet service', 'Too many mountains', 'Dirty drinking water', 'No school building'], 2, '细节题', '开头明确村庄网络信号较弱'],
      ['What do the students present to the company?', ['A stone tower', 'An old textbook', 'A village meal', 'Evidence about weak signals'], 3, '细节题', '他们收集记录和地图并作为证据提交'],
      ['Why does the writer replace stones with data?', ['To connect an old spirit with modern methods', 'To prove the old story is false', 'To advertise one company', 'To make teamwork unnecessary'], 0, '推理题', '改编保留坚持精神，同时使用数据和沟通解决现代问题'],
      ['What is the passage mainly about?', ['How towers are designed', 'A modern retelling about determination', 'Why villages avoid technology', 'The history of internet companies'], 1, '主旨题', '全文介绍愚公移山的现代改编及其核心精神'],
      ['“Determination” is closest in meaning to ___.', ['fear of change', 'technical knowledge', 'a firm decision to continue', 'a sudden lucky result'], 2, '词义猜测题', '与patience及持续行动并列，表示坚持到底的决心'],
    ],
  },
  {
    passage: 'The river dolphin project began after students learned that fewer dolphins were being seen near their city. Researchers explained that boat noise, plastic waste and changes in water quality all affected the animals. The students could not solve the entire problem, but they created a map where residents could report dolphin sightings and rubbish. After six months, the reports helped scientists identify two important feeding areas. The city then limited boat speed nearby and added waste bins along the river. Sightings have not risen yet, but researchers say reliable data is the first step toward effective protection.',
    rows: [
      ['What three problems affected the dolphins?', ['Heat, fishing and bridges', 'Noise, plastic and water changes', 'Tourists, maps and bins', 'Rain, food and scientists'], 3, '细节题', '研究人员列出船噪、塑料垃圾和水质变化'],
      ['What did the reports help scientists identify?', ['Two feeding areas', 'Six new dolphins', 'A faster boat route', 'A school building'], 0, '细节题', '报告帮助确认了两个重要觅食区'],
      ['Why does the writer mention sightings have not risen?', ['To show the project failed', 'To give a realistic view of progress', 'To blame the students', 'To end all protection work'], 1, '推理题', '作者承认短期数量未变，同时强调可靠数据的长期价值'],
      ['What is the best title?', ['How to Drive a Fast Boat', 'A City Without Wildlife', 'Student Data Supports Dolphin Protection', 'Why Maps Are Always Correct'], 2, '主旨题', '标题需概括学生收集数据并推动保护措施'],
      ['“Reliable” most nearly means ___.', ['colorful', 'expensive', 'secret', 'able to be trusted'], 3, '词义猜测题', '科学保护的第一步需要可信赖的数据'],
    ],
  },
  {
    passage: 'CITY HISTORY MUSEUM VISITOR GUIDE: The museum opens from 9:30 to 17:00 Tuesday through Sunday. The first floor tells the story of the old port; the second floor explores family life in the 1900s. The invention lab on the third floor requires a free timed ticket, available at the front desk. Large bags must be left in lockers. Photography is allowed without flash, except in the special painting room. At 14:00, volunteers lead a forty-minute tour beginning beside the information desk. Visitors under twelve must stay with an adult. The quiet room near Exit B is available to anyone who needs a break.',
    rows: [
      ['On which day is the museum closed?', ['Monday', 'Tuesday', 'Saturday', 'Sunday'], 0, '细节题', '开放时间为周二至周日，因此周一闭馆'],
      ['What is required for the invention lab?', ['A paid tour', 'A free timed ticket', 'A large locker', 'A flash camera'], 1, '细节题', '指南明确说三楼发明实验室需要免费分时票'],
      ['Where should a visitor wait for the 14:00 tour?', ['Inside the quiet room', 'At the painting room', 'Beside the information desk', 'Outside Exit B'], 2, '推理题', '导览从信息台旁开始，因此应在那里等候'],
      ['What is the guide mainly designed to do?', ['Describe one old painting', 'Compare two city ports', 'Sell family inventions', 'Help visitors plan a safe visit'], 3, '主旨题', '全文提供时间、楼层、票务和规则以帮助规划参观'],
      ['In this guide, “except” means ___.', ['not including', 'close to', 'because of', 'the same as'], 0, '词义猜测题', '除特殊绘画室外均可拍照，except表示不包括'],
    ],
  },
  {
    passage: 'Fast fashion makes new styles cheap and quickly available, but the low price can hide other costs. Producing large amounts of clothing uses water and energy. Some unwanted clothes are worn only a few times before being thrown away. Buying nothing is not the only answer. Consumers can check whether an item matches clothes they already own, choose stronger materials and repair small damage. Clothing exchanges also keep useful items in use. A thoughtful purchase may cost more at first, yet cost less per wear. The key question is not simply “Can I buy it?” but “Will I use it well?”',
    rows: [
      ['What resources does clothing production use?', ['Paper and glass', 'Water and energy', 'Wood and stone', 'Light and sound'], 1, '细节题', '文章明确指出大量制衣消耗水和能源'],
      ['What can keep useful clothes in use?', ['Changing prices', 'Following every style', 'Clothing exchanges', 'Hiding small damage'], 2, '细节题', '文中直接说衣物交换可延长有用衣物的使用'],
      ['Why might a stronger item cost less per wear?', ['It is always on sale', 'It needs more water', 'It changes fashion faster', 'It can be used many more times'], 3, '推理题', '耐用衣物使用次数更多，因此分摊到每次穿着的成本更低'],
      ['What does the writer mainly encourage?', ['Thoughtful and lasting consumption', 'Buying every new fashion', 'Throwing away old clothes', 'Choosing only the lowest price'], 0, '主旨题', '全文鼓励考虑搭配、耐用、修补和实际使用的理性消费'],
      ['The word “hide” in the first sentence means ___.', ['put under clothing', 'make less easy to notice', 'keep physically warm', 'remove from a shop'], 1, '词义猜测题', '低价使环境等其他成本不易被消费者注意'],
    ],
  },
];

export const GRADE8_READING_QUESTIONS = LEVELS.map((level, levelIndex) => ({
  level: levelIndex + 1,
  passScore: 3,
  questions: level.rows.map(([question, options, correctIndex, point, basis], questionIndex) => ({
    id: `grade8_reading_${levelIndex + 1}_${questionIndex + 1}`,
    passage: level.passage,
    question,
    options,
    correctIndex,
    explanation: `本题考查${point}。${basis}；其他选项或与原文矛盾，或缺少文本依据。`,
    type: 'choice',
  })),
}));
