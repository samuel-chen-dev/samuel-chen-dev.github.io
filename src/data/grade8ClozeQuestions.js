const LEVELS = [
  ['第一次独自旅行', 'Last spring, I {1} a train alone for the first time. I felt nervous, {2} the conductor kindly showed me my seat. I put my bag {3} the seat and checked the station names carefully. The trip taught me to act {4} when facing a new situation. When I finally saw my aunt, worry turned into {5}.', [
    [['take','took','will take','am taking'],1,'一般过去时','last spring确定过去时间，take的过去式took正确'],
    [['because','unless','so','but'],3,'转折连词','前后“紧张”和“得到帮助”构成转折，用but'],
    [['under','during','across','without'],0,'方位介词','行李放在座位下面用under'],
    [['independent','independence','independently','depending'],2,'副词修饰动词','修饰act需要副词independently'],
    [['anger','joy','fear','doubt'],1,'上下文逻辑','见到亲人后担忧变为喜悦joy最合情境'],
  ]],
  ['朋友间的误会', 'Mia did not reply to my message, so I {1} she was angry. I wanted to ask her, {2} I was afraid of making things worse. The next day, I spoke {3} her after class. She explained that her phone had stopped {4}. We both laughed and agreed that honest communication prevents a small doubt from becoming a serious {5}.', [
    [['think','thought','have thought','will think'],1,'过去叙事时态','全文叙述过去事件，应使用thought'],
    [['and','because','but','unless'],2,'转折关系','想询问却又害怕，前后构成转折'],
    [['to','from','with','over'],0,'动词介词搭配','speak to somebody表示与某人交谈'],
    [['work','worked','to work','working'],3,'stop doing结构','手机停止运行用stop working'],
    [['problem','present','program','project'],0,'语篇逻辑','误会可能发展成严重问题problem'],
  ]],
  ['校园科技节', 'Our class {1} a water-saving machine for the science festival. The first model failed, {2} we did not give up. We worked {3} lunch breaks and tested each part again. After several changes, the machine operated {4} and used half as much water. The judges praised not only our invention but also our spirit of {5}.', [
    [['builds','built','is building','will build'],1,'一般过去时','科技节项目是已完成事件，用built'],
    [['or','so','but','if'],2,'转折连词','模型失败与不放弃形成转折'],
    [['during','below','among','toward'],0,'时间介词','在午休期间用during'],
    [['success','successful','successfully','succeed'],2,'副词词性','修饰operated应用successfully'],
    [['curiosity','silence','competition','teamwork'],3,'上下文逻辑','共同修改测试体现团队合作teamwork'],
  ]],
  ['社区旧物交换', 'Our community {1} a swap day last Sunday. People brought useful things they no longer needed, {2} others could take them home. I exchanged two books {3} a desk lamp. The event was carefully {4}, with labels for every kind of item. It showed that an old object can still have {5} for someone else.', [
    [['holds','held','has held','will hold'],1,'过去时态','last Sunday要求held'],
    [['although','so that','unless','before'],1,'目的关系','带来旧物是为了让别人使用，用so that'],
    [['for','to','by','with'],0,'exchange搭配','exchange A for B表示用A换B'],
    [['organize','organization','organized','organizing'],2,'被动语态词形','活动被组织，应使用过去分词organized'],
    [['value','noise','danger','temperature'],0,'上下文逻辑','有用旧物对别人仍有价值value'],
  ]],
  ['暴雨中的抉择', 'When the storm began, Leo {1} home from basketball practice. He could wait in a shop, {2} he noticed a younger child alone outside. Leo stood {3} the child and the road to protect him from traffic. He calmly called the boy’s parents and gave a clear {4} of their location. His safe choice proved that courage also means making a {5} decision.', [
    [['walked','was walking','has walked','will walk'],1,'过去进行时','暴雨开始时正在回家，用was walking'],
    [['because','if','but','until'],2,'转折连词','本可等待却注意到孩子，使用but'],
    [['between','under','through','without'],0,'位置介词','站在孩子和道路之间用between'],
    [['describe','description','descriptive','describing'],1,'词性辨析','冠词和形容词后需要名词description'],
    [['careless','responsible','ordinary','private'],1,'语境逻辑','保护孩子并求助体现负责任的决定'],
  ]],
  ['坚持阅读的改变', 'Nina {1} disliked reading because she read slowly. Her teacher suggested short stories, {2} she started with ten minutes a day. She kept a dictionary {3} her so that she could check important words. After six months, her reading speed had {4} greatly. More importantly, books had become a source of ideas rather than a {5}.', [
    [['use to','used to','is used to','has used to'],1,'used to结构','过去常常不喜欢阅读用used to'],
    [['so','but','unless','although'],0,'因果连词','老师建议导致开始练习，用so'],
    [['beside','across','against','during'],0,'方位介词','字典放在身旁用beside'],
    [['improvement','improving','improve','improved'],3,'完成时词形','had后接过去分词improved'],
    [['pleasure','burden','choice','method'],1,'对比逻辑','rather than连接相反概念，书不再是负担burden'],
  ]],
  ['保护城市河流', 'For years, rubbish {1} into the Blue River after heavy rain. A student group investigated the problem {2} they wanted evidence, not guesses. They placed collection nets {3} three busy drains and recorded the waste. Their report was clear and {4}, so the city added filters. The project showed that reliable data can turn concern into effective {5}.', [
    [['washed','was washed','is washing','has wash'],1,'一般过去时被动','垃圾被冲入河流，应用was washed'],
    [['because','but','unless','while'],0,'原因连词','想要证据解释调查原因，用because'],
    [['near','during','without','except'],0,'地点介词','收集网设置在排水口附近用near'],
    [['persuade','persuasion','persuasive','persuasively'],2,'形容词词性','与clear并列作表语应用persuasive'],
    [['action','argument','memory','custom'],0,'上下文逻辑','数据促成过滤措施，即把关切转化为行动action'],
  ]],
  ['一场失败的演讲', 'I had practiced for days, but my mind {1} blank when I faced the audience. Instead of leaving, I paused {2} took a slow breath. A friend smiled at me {3} the front row. I restarted with a simpler opening and spoke more {4}. I did not win, yet finishing the speech gave me the {5} to try again.', [
    [['goes','went','has gone','will go'],1,'过去时态','演讲经历发生在过去，用went blank'],
    [['and','or','unless','although'],0,'顺承连词','停顿并深呼吸是连续动作，用and'],
    [['in','on','at','by'],0,'位置介词','在前排用in the front row'],
    [['confidence','confident','confidently','confide'],2,'副词词性','修饰spoke需要confidently'],
    [['courage','result','prize','excuse'],0,'情感逻辑','完成失败后的演讲给予再次尝试的勇气'],
  ]],
  ['无人机送药', 'A mountain road {1} by snow when a patient needed medicine. The hospital contacted a drone team, {2} driving was impossible. The medicine was placed {3} a heated box to protect it from the cold. The drone flew {4} through strong wind and arrived in twenty minutes. Technology succeeded because trained people made careful choices at every {5}.', [
    [['blocked','was blocked','has blocking','will block'],1,'过去时被动','道路被雪封锁，用was blocked'],
    [['because','although','unless','while'],0,'原因连词','无法驾车解释联系无人机团队的原因'],
    [['inside','above','across','beside'],0,'空间介词','药物放在保温箱里面用inside'],
    [['steady','steadiness','steadily','steadied'],2,'副词词性','修饰flew需要steadily'],
    [['stage','story','direction','distance'],0,'语篇逻辑','训练人员在每个阶段作出选择，用stage概括流程'],
  ]],
];

function balanceOptions(options, correctIndex, targetIndex) {
  const correct = options[correctIndex];
  const balanced = options.filter((_, index) => index !== correctIndex);
  balanced.splice(targetIndex, 0, correct);
  return balanced;
}

export const GRADE8_CLOZE_QUESTIONS = LEVELS.map(([title, template, blanks], levelIndex) => ({
  level: levelIndex + 1,
  passScore: 3,
  questions: blanks.map(([options, correctIndex, point, reason], questionIndex) => ({
    id: `grade8_cloze_${levelIndex + 1}_${questionIndex + 1}`,
    passage: template.replace(/\{(\d)\}/g, (_, number) => Number(number) === questionIndex + 1 ? '___' : blanks[Number(number) - 1][0][blanks[Number(number) - 1][1]]),
    question: `${title}：请选择最适合填入空格的选项。`,
    options: balanceOptions(options, correctIndex, (levelIndex * 5 + questionIndex) % 4),
    correctIndex: (levelIndex * 5 + questionIndex) % 4,
    explanation: `本题考查${point}。${reason}；其余选项在时态、搭配、词性或上下文意义上不符合该语篇。`,
    type: 'choice',
  })),
}));
