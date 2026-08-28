function balance(options, answer, index) {
  const correct = options[answer];
  const arranged = options.filter((_, i) => i !== answer);
  const correctIndex = index % 4;
  arranged.splice(correctIndex, 0, correct);
  return { options: arranged, correctIndex };
}

function build(moduleId, levels) {
  return levels.map(({ topic, rows }, li) => ({ level: li + 1, passScore: 3, questions: rows.map(([question, options, answer, reason], qi) => ({ id: `zhongkao_${moduleId}_${li + 1}_${qi + 1}`, question, ...balance(options, answer, li * 5 + qi), explanation: `本题考查${topic}。${reason}；其他选项不满足句法、搭配或语篇逻辑。`, type: moduleId === 'grammar' ? 'grammar' : 'choice' })) }));
}

const grammar = [
  { topic: '时态与语态整合', rows: [
    ['By the time we arrived, the film ___.',['started','had started','has started','starts'],1,'by the time引导过去时间，先发生的动作使用过去完成时'],
    ['A new bridge ___ over the river next year.',['builds','built','will be built','has built'],2,'主语bridge承受动作，且时间是将来'],
    ['I ___ this book twice, so I can discuss it with you.',['read','was reading','had read','have read'],3,'twice与当前结果提示现在完成时'],
    ['While Mum ___ dinner, the lights went out.',['was cooking','cooks','has cooked','is cooking'],0,'过去某时正在进行的背景动作使用过去进行时'],
    ['The classroom ___ before the parents arrived yesterday.',['cleans','had been cleaned','was cleaning','has cleaned'],1,'教室被打扫且发生在arrived之前'],
  ]},
  { topic: '非谓语动词', rows: [
    ['The teacher advised us ___ notes while reading.',['take','taking','to take','took'],2,'advise somebody to do something'],
    ['___ the early bus, Leo left home at six.',['Catch','Caught','Catching','To catch'],3,'不定式置于句首表示目的'],
    ['The girl ___ by the window is my cousin.',['standing','stood','stands','to stand'],0,'现在分词作后置定语表示主动进行'],
    ['I look forward to ___ from you soon.',['hear','hearing','heard','to hear'],1,'look forward to中的to是介词，后接动名词'],
    ['The homework must be ___ before Friday.',['finish','finishing','finished','to finish'],2,'被动结构be后接过去分词'],
  ]},
  { topic: '情态动词表推测', rows: [
    ['The lights are on. Someone ___ be at home.',['can’t','mustn’t','shouldn’t','must'],3,'有明确迹象时肯定推测用must'],
    ['That ___ be Lucy; she has gone to Shanghai.',['can’t','must','may','should'],0,'已去上海说明不可能是Lucy'],
    ['The key is not here. Dad ___ have taken it, but I’m not sure.',['must','may','can’t','need'],1,'不确定的过去推测用may have done'],
    ['You ___ be tired after such a long journey.',['can','might not','must','needn’t'],2,'根据长途旅行作有把握的推测'],
    ['This coat ___ belong to Sam. His is much smaller.',['must','should','may','can’t'],3,'尺寸明显不符，作否定推测'],
  ]},
  { topic: '定语从句', rows: [
    ['The woman ___ spoke at the meeting is our principal.',['who','which','whose','where'],0,'先行词woman指人且关系词作主语'],
    ['This is the camera ___ I bought online.',['who','which','where','when'],1,'先行词camera指物，关系词作宾语'],
    ['I visited the village ___ my grandfather was born.',['which','that','where','who'],2,'关系词在从句中作地点状语'],
    ['The student ___ project won the prize is from Class Two.',['who','whom','which','whose'],3,'关系词表示所属关系'],
    ['Everything ___ we need is in this box.',['that','who','where','whose'],0,'先行词为不定代词everything，常用that'],
  ]},
  { topic: '宾语从句', rows: [
    ['Could you tell me ___?',['where is the station','where the station is','the station is where','where was the station'],1,'宾语从句使用陈述语序'],
    ['I wonder ___ he will join us tomorrow.',['that','what','whether','where did'],2,'表达是否使用whether'],
    ['The teacher said that light ___ faster than sound.',['traveled','was traveling','has traveled','travels'],3,'客观真理不受主句过去时限制'],
    ['Nobody knew why the meeting ___.',['had been canceled','has canceled','cancels','will cancel'],0,'会议被取消且先于过去的knew'],
    ['Do you know when the train ___ tomorrow?',['left','leaves','will leave','leaving'],1,'时刻表可用一般现在时表将来'],
  ]},
  { topic: '状语从句', rows: [
    ['We will stay at home if it ___ tomorrow.',['rained','will rain','rains','is raining'],2,'条件状语从句遵循主将从现'],
    ['___ he was tired, he continued working.',['Because','If','Unless','Although'],3,'前后为让步关系'],
    ['Please call me as soon as you ___ there.',['arrive','will arrive','arrived','are arriving'],0,'时间状语从句用一般现在时表将来'],
    ['You cannot improve ___ you practice regularly.',['if','unless','because','so'],1,'unless表示除非'],
    ['The box was ___ heavy that I could not lift it.',['such','too','so','very'],2,'so加形容词加that从句'],
  ]},
  { topic: '主谓一致', rows: [
    ['Neither Tom nor his parents ___ at home now.',['is','was','be','are'],3,'neither nor遵循就近原则，parents为复数'],
    ['The number of visitors ___ increasing.',['is','are','have','were'],0,'the number of作主语，谓语单数'],
    ['A number of students ___ volunteered.',['has','have','is','was'],1,'a number of加复数名词，谓语复数'],
    ['Each of the answers ___ worth checking.',['are','have','is','were'],2,'each作主语，谓语单数'],
    ['Not only the students but also the teacher ___ excited.',['were','have','are','was'],3,'就近主语teacher为单数且语境过去'],
  ]},
  { topic: '特殊句式', rows: [
    ['___ useful advice the coach gave us!',['What','How','What a','How a'],0,'advice不可数，使用What加形容词加名词'],
    ['You have finished the report, ___?',['do you','haven’t you','don’t you','have you'],1,'前肯后否，助动词与现在完成时一致'],
    ['Only then ___ the importance of teamwork.',['I understood','I did understand','did I understand','understood I'],2,'only加状语置于句首引起部分倒装'],
    ['___ quickly the rescue team arrived!',['What','What a','How a','How'],3,'修饰副词quickly使用How'],
    ['Never ___ such a moving speech before.',['have I heard','I have heard','did I heard','I heard'],0,'否定副词置首引起部分倒装'],
  ]},
  { topic: '中考语篇纠错', rows: [
    ['Choose the correct sentence.',['She suggested to take a bus.','She suggested taking a bus.','She suggested us take a bus.','She suggested took a bus.'],1,'suggest后接动名词'],
    ['Choose the correct sentence.',['The news are exciting.','The news were excite.','The news is exciting.','The news is excited.'],2,'news是不可数名词，事物令人兴奋用exciting'],
    ['Choose the correct sentence.',['He has left for two hours.','He left since two hours.','He has been leaving for two hours.','He has been away for two hours.'],3,'与时间段连用需使用延续性状态be away'],
    ['Choose the correct sentence.',['If it doesn’t rain, we will go hiking.','If it won’t rain, we go hiking.','Unless it doesn’t rain, we will go.','If not rain, we will hike.'],0,'条件从句使用一般现在时，主句使用将来时'],
    ['Choose the correct sentence.',['The boy which helped me was kind.','The boy who helped me was kind.','The boy helped me who was kind.','The boy whom helped me was kind.'],1,'指人且关系词作主语用who'],
  ]},
];

const vocabulary = [
  { topic: '构词法', rows: [
    ['The suffix “-less” in “careless” means ___.',['without','full of','again','before'],0,'-less表示缺少或没有'],
    ['The opposite of “possible” is ___.',['unpossible','impossible','dispossible','nonpossible'],1,'possible的否定前缀为im-'],
    ['A person who invents things is an ___.',['invention','invent','inventor','inventive'],2,'-or构成表示人的名词'],
    ['“Rebuild” means ___.',['build badly','not build','build together','build again'],3,'前缀re-表示再次'],
    ['The noun form of “decide” is ___.',['decision','decisive','deciding','decided'],0,'decide对应名词decision'],
  ]},
  { topic: '近义动词辨析', rows: [
    ['Please ___ me your new address.',['say','tell','speak','talk'],1,'tell somebody something'],
    ['The teacher ___ the class into four groups.',['separated','connected','divided','included'],2,'divide into是固定搭配'],
    ['Can I ___ your dictionary for a day?',['lend','keep','offer','borrow'],3,'从别人处借入用borrow'],
    ['The heavy rain ___ us from leaving.',['prevented','protected','provided','prepared'],0,'prevent somebody from doing'],
    ['The red scarf ___ her black coat.',['fits','matches','suits size','meets'],1,'颜色与衣服搭配用match'],
  ]},
  { topic: '形容词与副词', rows: [
    ['The problem is ___ difficult for beginners.',['high','deep','especially','widely'],2,'especially修饰形容词difficult'],
    ['The doctor examined the patient ___.',['careful','care','carefulness','carefully'],3,'副词修饰动词examined'],
    ['This book is ___ worth reading.',['well','good','better','best'],0,'be well worth doing是固定表达'],
    ['The road became ___ after the snow.',['dangerously','dangerous','danger','endanger'],1,'系动词became后接形容词'],
    ['The two plans are ___ different.',['complete','completion','completely','completing'],2,'副词completely修饰different'],
  ]},
  { topic: '介词短语', rows: [
    ['The flight was canceled ___ the storm.',['although','despite','because','because of'],3,'because of后接名词短语'],
    ['___ my surprise, everyone agreed.',['To','At','In','For'],0,'to one’s surprise为固定短语'],
    ['The project was completed ___ time.',['at','on','by','from'],1,'on time表示准时'],
    ['We solved the problem ___ the help of our teacher.',['under','through','with','by'],2,'with the help of为固定表达'],
    ['The village is famous ___ its tea.',['at','to','with','for'],3,'be famous for表示因某物闻名'],
  ]},
  { topic: '动词短语', rows: [
    ['Please ___ the lights before leaving.',['turn off','turn on','turn up','turn into'],0,'离开前应关闭灯'],
    ['We must ___ a solution before Friday.',['look after','come up with','run out of','look down on'],1,'come up with表示想出'],
    ['The match was ___ because of rain.',['put up','taken off','put off','given away'],2,'put off表示推迟'],
    ['Do not ___ when facing a difficult task.',['set out','take over','show off','give up'],3,'give up表示放弃'],
    ['The old factory has been ___ a museum.',['turned into','turned down','turned off','turned over'],0,'turn into表示变成'],
  ]},
  { topic: '学术高频词', rows: [
    ['The survey aims to ___ students’ reading habits.',['invent','analyze','borrow','decorate'],1,'analyze表示分析数据或信息'],
    ['The results ___ that sleep affects memory.',['refuse','imagine','indicate','remove'],2,'indicate表示研究结果表明'],
    ['Researchers collected enough ___ to support the idea.',['advice','space','progress','evidence'],3,'evidence表示支持观点的证据'],
    ['The experiment requires a careful ___.',['method','message','festival','journey'],0,'method表示研究方法'],
    ['We should ___ the advantages of both plans.',['produce','compare','cancel','hide'],1,'compare表示比较'],
  ]},
  { topic: '熟词生义', rows: [
    ['In “The hall can hold 500 people”, “hold” means ___.',['抓住','举办','容纳','坚持'],2,'此处指大厅的容量'],
    ['In “The story moved me”, “moved” means ___.',['搬家','移动','推动','感动'],3,'故事使人产生情感，意为感动'],
    ['In “a light meal”, “light” means ___.',['少量清淡的','明亮的','浅色的','轻便的'],0,'修饰meal时表示清淡少量'],
    ['In “book a room”, “book” means ___.',['阅读','预订','书写','出版'],1,'book作动词表示预订'],
    ['In “a hard question”, “hard” means ___.',['坚硬的','努力地','困难的','猛烈地'],2,'修饰question表示困难'],
  ]},
  { topic: '逻辑连接词', rows: [
    ['The plan is cheap; ___, it may cause pollution.',['therefore','for example','besides','however'],3,'前后形成转折'],
    ['The road was closed; ___, we took another route.',['therefore','however','meanwhile','instead of'],0,'前因导致后果'],
    ['Many activities reduce stress. ___, walking can calm us.',['In conclusion','For example','Otherwise','However'],1,'后句举例说明'],
    ['The device is small. ___, it is easy to carry.',['In contrast','Nevertheless','Moreover','Instead'],2,'后句补充另一优点'],
    ['Wear a coat; ___, you may catch a cold.',['therefore','besides','for instance','otherwise'],3,'otherwise表示否则'],
  ]},
  { topic: '语境词汇综合', rows: [
    ['The volunteers showed great ___ by continuing in the rain.',['determination','description','direction','discussion'],0,'冒雨坚持体现决心'],
    ['Good communication can prevent unnecessary ___.',['success','misunderstandings','agreements','progress'],1,'良好沟通可避免误会'],
    ['The museum uses technology to make history more ___.',['silent','private','accessible','ordinary'],2,'技术让历史更容易接触理解'],
    ['Before sharing a story online, check whether the source is ___.',['creative','popular','recent','reliable'],3,'分享前应核查来源是否可靠'],
    ['The project had a positive ___ on the community.',['impact','entrance','excuse','method'],0,'have an impact on是固定搭配'],
  ]},
];

export const ZHONGKAO_GRAMMAR_QUESTIONS = build('grammar', grammar);
export const ZHONGKAO_VOCABULARY_QUESTIONS = build('vocabulary', vocabulary);
