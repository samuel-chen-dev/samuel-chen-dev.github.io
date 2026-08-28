const LEVELS = [
  ['比较与欣赏朋友', [
    ['Your friend says, “I finally finished the race!” You reply:', ['Well done! I knew you could do it.', 'The race was not important.', 'Why were you so slow?', 'I did not watch you.'],0,'祝贺朋友','Well done能真诚肯定朋友的努力'],
    ['“How is Tina different from you?” “She is ___, but we are both helpful.”', ['as tall as me','more outgoing than me','the most outgoing','too outgoing to talk'],1,'人物比较','more outgoing than me正确表达两人性格差异'],
    ['Which comment gives a balanced comparison?', ['Ben is simply better than Leo.','Leo has nothing in common with Ben.','Ben speaks more, while Leo listens carefully.','Both boys must change their personalities.'],2,'客观比较','该句分别呈现两人的优点，表达具体且尊重'],
    ['A classmate feels inferior to an excellent friend. What is the best response?', ['Stop being friends with her.','Try to defeat her in everything.','Pretend you do not care.','Learn from her strengths and value your own.'],3,'支持性回应','既欣赏他人长处又肯定自己最具建设性'],
    ['“Sam is quieter than I am, so we cannot be close friends.” What is the best reply?', ['Different personalities can still work well together.','Quiet people never need friends.','Only similar people understand each other.','You should make Sam talk more.'],0,'反驳刻板判断','性格不同不妨碍相互理解和互补'],
  ]],
  ['影视节目偏好', [
    ['“What do you think of the documentary?” “___”', ['I watched it yesterday.','It was informative and moving.','It starts at eight.','It is on Channel Five.'],1,'表达评价','informative and moving直接回答对节目的看法'],
    ['“Why do you prefer comedies?” “___”', ['They are two hours long.','My brother prefers news.','They help me relax after school.','The cinema is nearby.'],2,'说明偏好原因','放松身心是选择喜剧的合理原因'],
    ['How can you disagree politely?', ['You are completely wrong.','That show is terrible.','Nobody agrees with you.','I see your point, but I prefer the book.'],3,'礼貌表达异议','先认可对方观点再说明个人偏好，语气得体'],
    ['“The ending was surprising.” Which reply keeps the discussion going?', ['What made it surprising to you?','Yes, the film has an ending.','I have a television at home.','Surprises are always bad.'],0,'追问与延续对话','开放式追问能邀请对方解释观点'],
    ['“The effects were amazing, so it must be a great film.” Best critical response:', ['Effects never matter.','Great effects help, but the story matters too.','Every expensive film is excellent.','We should only discuss actors.'],1,'有依据地评价','回应特效优点并补充故事标准，观点更全面'],
  ]],
  ['职业理想与计划', [
    ['“What do you want to be?” “___”', ['I was at school.','I like weekends.','I hope to become an engineer.','I have finished lunch.'],2,'表达职业理想','hope to become清楚表达未来职业目标'],
    ['“How will you achieve that goal?” “___”', ['The goal is difficult.','My parents have jobs.','Engineers use computers.','I will study math and join the robotics club.'],3,'说明行动计划','回答包含与目标相关的具体学习和实践步骤'],
    ['Which question explores someone’s motivation?', ['Why does that career interest you?','Where is your classroom?','When did the film end?','How much is the ticket?'],0,'追问职业动机','why直接询问选择该职业的原因'],
    ['“I may change my dream later.” Best response:', ['Then never make a plan.','That is okay; plans can develop as you learn.','A dream must never change.','Choose the most popular job now.'],1,'回应不确定性','允许计划随认识深化而调整，语气支持且现实'],
    ['Which answer presents a complete plan?', ['I want a useful job.','Science is interesting.','I aim to be a doctor, so I will study biology and volunteer at a clinic.','My future will probably be fine.'],2,'目标与路径表达','该句同时交代目标、学习内容和实践行动'],
  ]],
  ['邀请与婉拒', [
    ['“Would you like to come to my party?” “___”', ['It is a party.','You have a house.','Saturday is a day.','Yes, I’d love to.'],3,'接受邀请','Yes, I’d love to是自然礼貌的接受方式'],
    ['Which is a polite refusal?', ['I’m sorry, but I have a piano lesson then.','No. Your party sounds boring.','I never attend your events.','Do not ask me again.'],0,'婉拒邀请','先道歉再说明时间冲突，给对方充分尊重'],
    ['“I’m busy on Friday.” “___”', ['Then cancel your work.','Would Saturday afternoon work for you?','Why are you always busy?','The activity is on Friday.'],1,'协商替代时间','主动提出周六作为替代方案以继续协调'],
    ['You have not decided yet. What should you say?', ['I will certainly come.','I refuse the invitation.','May I check my schedule and reply tonight?','Please wait for several weeks.'],2,'暂缓回应','说明需查看日程并给出明确回复时间'],
    ['A friend invites you, but you promised to help your sister. Best response:', ['My sister can wait.','I cannot go, goodbye.','Your event is less important.','Thanks for inviting me. Could we meet another day?'],3,'冲突日程下的得体回应','感谢、解释不能参加并提出替代安排最完整'],
  ]],
  ['看病与安全', [
    ['“What’s the matter?” “___”', ['I have a bad headache.','I enjoy basketball.','The room is large.','I went by bus.'],0,'描述症状','have a headache直接说明身体不适'],
    ['“I cut my finger.” What is sensible advice?', ['Run as fast as possible.','Wash it and cover it with a clean bandage.','Touch it with dirty hands.','Ignore heavy bleeding.'],1,'基础急救建议','清洗并用干净绷带覆盖适合小伤口'],
    ['Which question should a doctor ask first?', ['What films do you like?','Where did you buy shoes?','How long have you felt this way?','Who won the game?'],2,'问诊关键信息','症状持续时间是判断病情的重要信息'],
    ['Someone may have broken an arm. Best response:', ['Pull the arm straight.','Ask them to carry a bag.','Wait several days silently.','Keep it still and get medical help.'],3,'受伤安全处置','固定伤处并寻求专业帮助可避免二次伤害'],
    ['“Can I return to training today?” Best responsible reply:', ['Follow the doctor’s advice instead of rushing back.','Train harder to forget the pain.','Medicine makes every activity safe.','Ask a friend to decide for you.'],0,'负责任的健康建议','是否恢复训练应遵循医生判断而非急于返回'],
  ]],
  ['许可与家务分工', [
    ['“Could I use your laptop?” “___”', ['It is expensive.','Sure, but please be careful with it.','I used it yesterday.','Laptops need power.'],1,'回应许可请求','同意并提出合理使用条件，回应完整'],
    ['Which request is the most polite?', ['Clean the kitchen now.','You must do my work.','Could you please help me clean the kitchen?','Why is the kitchen dirty?'],2,'礼貌请求','Could you please结构表达请求而非命令'],
    ['“I cooked dinner. Could you wash the dishes?” “___”', ['Cooking is easy.','Dishes are made of glass.','I washed them last week.','Of course. That sounds fair.'],3,'公平分工回应','认可对方贡献并接受对应任务体现公平'],
    ['You cannot finish your chore on time. Best response:', ['Explain early and suggest when you can do it.','Hide the unfinished work.','Blame another family member.','Wait until someone complains.'],0,'协商责任','提前说明并给出新的完成时间体现负责'],
    ['“Why should everyone share chores?” Best answer:', ['Because chores disappear alone.','Because shared work is fair and builds responsibility.','Because young people need no rest.','Because only difficult chores matter.'],1,'解释分工价值','公平和责任感是共同承担家务的合理理由'],
  ]],
  ['冲突、建议与道歉', [
    ['Your friend looks upset. What should you ask?', ['Why are you so difficult?','Did you lose the game?','Would you like to tell me what happened?','You should stop feeling sad.'],2,'关怀式询问','开放且不带判断的问题给朋友表达空间'],
    ['A classmate broke your pen by accident. Best reply to the apology:', ['Buy me ten new pens.','I will never trust you.','You always break things.','I understand. Please be more careful next time.'],3,'接受道歉并提出边界','既接受意外又清楚提出以后注意'],
    ['Which is a sincere apology?', ['I’m sorry I shared your photo without asking. I won’t do it again.','I’m sorry you became angry.','Maybe you misunderstood me.','Everyone shares photos.'],0,'承担责任的道歉','明确指出自己的行为并承诺改正才是真诚道歉'],
    ['Two friends are arguing. What is constructive advice?', ['Choose a winner immediately.','Let each person speak without interruption.','Tell them to post it online.','Avoid both friends forever.'],1,'冲突调解建议','轮流完整表达能帮助理解事实和感受'],
    ['“I apologized, so why is she still upset?” Best response:', ['She must forgive you now.','Your apology was useless.','Give her time and show change through your actions.','Ask others to pressure her.'],2,'道歉后的修复','对方需要时间，持续行为改变比要求立即原谅更恰当'],
  ]],
  ['讲述突发事件', [
    ['Which sentence sets the background?', ['The alarm rang once.','We called the police.','Nobody was hurt.','It was raining while we were walking home.'],3,'叙事背景表达','过去进行时描述突发事件发生前持续的背景'],
    ['“What happened next?” “___”', ['A cyclist suddenly fell near the crossing.','The crossing is beside a bank.','Cycling is a useful sport.','I usually walk to school.'],0,'推进事件叙述','suddenly和过去式清楚引入下一突发动作'],
    ['Which pair correctly shows interruption?', ['I called while the bell rang.','We were waiting when the lights went out.','They wait when the rain was starting.','She was arrive while we talked.'],1,'长短动作组织','were waiting是背景长动作，went out是插入的短动作'],
    ['A listener asks, “Were you frightened?” Best complete response:', ['The street was dark.','My friend had a phone.','At first, yes, but I became calmer after help arrived.','Fear is an emotion.'],2,'叙述情感变化','回答问题并呈现从害怕到冷静的变化'],
    ['Which ending best reflects on an emergency?', ['Then everything ended.','It was an event.','We went home later.','The experience taught me to stay calm and call for help.'],3,'叙事反思','结尾总结从事件中得到的具体安全经验'],
  ]],
  ['手机进校园讨论', [
    ['How can you introduce your opinion?', ['In my view, phones can help if schools set clear rules.','Everyone knows I am right.','There is nothing to discuss.','Phones are rectangular devices.'],0,'提出观点','In my view自然引出有条件、有立场的意见'],
    ['Which sentence gives supporting evidence?', ['Phones are popular.','For example, students can photograph experiment results for reports.','Some phones are blue.','Technology changes quickly.'],1,'举例支持观点','具体学习用途能够作为允许手机的论据'],
    ['How do you respond to a reasonable opposing point?', ['That idea is foolish.','Stop disagreeing with me.','I understand the distraction risk, so phones could stay off during lessons.','Any rule will certainly fail.'],2,'回应异议并调整方案','先承认风险再提出课堂关闭的折中规则'],
    ['Which question helps a group reach a practical rule?', ['Who owns the newest phone?','Why do adults make rules?','Which brand is best?','When and where should phone use be allowed?'],3,'协商具体规则','询问允许的时间地点能把观点转化为可执行方案'],
    ['Which conclusion best summarizes a balanced discussion?', ['Phones may support learning, but use should be limited and supervised.','Phones should always be free to use.','All technology should leave school.','Only top students need rules.'],0,'总结协商结论','同时保留学习价值和必要限制，综合双方合理观点'],
  ]],
];

export const GRADE8_SPEAKING_QUESTIONS = LEVELS.map(([topic, rows], levelIndex) => ({
  level: levelIndex + 1, passScore: 3,
  questions: rows.map(([question, options, correctIndex, point, reason], questionIndex) => ({
    id: `grade8_speaking_${levelIndex + 1}_${questionIndex + 1}`, question, options, correctIndex,
    explanation: `本题考查${point}。${reason}；其他选项没有回应交际目的，或语气、内容在该场景中不够得体。`, type: 'choice',
  })),
}));
