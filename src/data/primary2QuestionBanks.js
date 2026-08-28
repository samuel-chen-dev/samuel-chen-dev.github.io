function balance(options, answer, index) {
  const correct = options[answer];
  const result = options.filter((_, optionIndex) => optionIndex !== answer);
  const correctIndex = index % 4;
  result.splice(correctIndex, 0, correct);
  return { options: result, correctIndex };
}

function choiceBank(moduleId, levels) {
  return levels.map((level, levelIndex) => ({
    level: levelIndex + 1,
    passScore: 3,
    questions: level.rows.map(([question, options, answer, reason], questionIndex) => ({
      id: `primary2_${moduleId}_${levelIndex + 1}_${questionIndex + 1}`,
      question,
      ...balance(options, answer, levelIndex * 5 + questionIndex),
      explanation: `本题考查${level.topic}。${reason}；其他选项在词义、词形、搭配或语境上不正确。`,
      type: 'choice',
    })),
  }));
}

const grammarLevels = [
  { topic: 'be动词与人称配合', rows: [
    ['I ___ in Class Three.', ['am','is','are','be'],0,'主语I固定搭配am'],
    ['My little brother ___ six years old.', ['am','are','is','be'],2,'单数主语My little brother搭配is'],
    ['Lucy and Lily ___ good friends.', ['is','am','be','are'],3,'并列主语表示复数，应使用are'],
    ['There ___ some milk and two apples on the table.', ['is','are','am','be'],0,'there be遵循就近原则，最近的milk不可数，用is'],
    ['Neither Tom nor his parents ___ at home now.', ['is','am','are','be'],2,'就近主语his parents是复数，所以用are'],
  ]},
  { topic: 'have和has表示所属', rows: [
    ['I ___ a new pencil box.', ['has','have','having','had'],1,'主语I使用have'],
    ['The cat ___ two blue eyes.', ['have','having','has','had'],2,'单数主语The cat使用has'],
    ['We ___ any juice at home.', ["don't have","doesn't have",'not has','has not'],0,'主语We的一般现在时否定用do not have'],
    ['___ your sister have a bike?', ['Do','Is','Has','Does'],3,'主语your sister是第三人称单数，疑问句用Does'],
    ['Tom and his brother each ___ a model plane.', ['has','have','having','to have'],0,'each强调每个人，谓语用单数has'],
  ]},
  { topic: '人称代词与物主代词', rows: [
    ['___ am a student.', ['Me','I','My','Mine'],1,'句中缺少主语，应使用主格I'],
    ['Please give the book to ___.', ['he','his','him','he is'],2,'介词to后使用宾格him'],
    ['This is Amy. ___ bag is pink.', ['She','Hers','Her','Herself'],2,'名词bag前用形容词性物主代词Her'],
    ['Our classroom is bigger than ___.', ['they','their','them','theirs'],3,'比较对象是“他们的教室”，用名词性物主代词theirs'],
    ['Lucy helped Tom and ___ with our project.', ['I','me','my','mine'],1,'介词with前并列宾语需要宾格me'],
  ]},
  { topic: '名词单复数', rows: [
    ['There are three ___ in the box.', ['toy','toys','toies','a toy'],1,'three后接可数名词复数toys'],
    ['Two ___ are playing under the tree.', ['childs','childes','children','child'],2,'child的不规则复数是children'],
    ['We need some ___ for the soup.', ['tomato','tomatos','a tomato','tomatoes'],3,'tomato的复数通常加-es'],
    ['How much ___ is there in the bottle?', ['water','waters','a water','bottles'],0,'water是不可数名词，不加复数'],
    ['The two ___ teachers are talking with three ___.', ["women; boys","woman; boys","women; boy","woman; boy"],0,'woman修饰复数名词时也变为women，three后用boys'],
  ]},
  { topic: '现在进行时', rows: [
    ['Look! The dog ___ after a ball.', ['runs','is running','ran','run'],1,'Look提示动作正在发生，用is running'],
    ['The children ___ pictures now.', ['draw','drew','are drawing','draws'],2,'now和复数主语要求are drawing'],
    ['Listen! Someone ___ at the door.', ['knock','knocks','knocked','is knocking'],3,'Listen提示当前正在敲门'],
    ['My father is ___ dinner in the kitchen.', ['making','makeing','makes','made'],0,'make去e加-ing，写作making'],
    ['Be quiet. The baby ___.', ['sleeps every day','is sleeping','slept yesterday','will sleep'],1,'Be quiet暗示婴儿此刻正在睡觉'],
  ]},
  { topic: '一般现在时与第三人称单数', rows: [
    ['I usually ___ to school by bike.', ['goes','going','go','went'],2,'usually表示习惯，主语I用动词原形go'],
    ['Mary ___ English every evening.', ['study','studying','studied','studies'],3,'Mary是第三人称单数，study变studies'],
    ['My parents ___ TV after dinner.', ['watch','watches','watching','watched'],0,'复数主语parents使用动词原形watch'],
    ['Tom does not ___ computer games on school days.', ['plays','play','played','playing'],1,'does not后必须接动词原形play'],
    ['The earth ___ around the sun.', ['move','moving','moves','moved'],2,'客观事实用一般现在时，单数主语用moves'],
  ]},
  { topic: '一般过去时与规则动词', rows: [
    ['We ___ the museum yesterday.', ['visit','visits','visiting','visited'],3,'yesterday要求一般过去时visited'],
    ['Amy ___ her room last Sunday.', ['cleaned','cleans','clean','cleaning'],0,'last Sunday表示过去，clean加-ed'],
    ['They ___ at home last night.', ['stay','stayed','stays','staying'],1,'last night要求stayed'],
    ['The bus ___ ten minutes ago.', ['stoped','stops','stopped','stopping'],2,'重读闭音节stop双写p再加-ed'],
    ['Did Jack ___ football after school?', ['played','plays','playing','play'],3,'助动词Did后接动词原形play'],
  ]},
  { topic: 'can、must与should', rows: [
    ['Birds ___ fly.', ['can','must','should','need'],0,'can表示鸟具有飞行能力'],
    ['You ___ stop when the traffic light is red.', ['can','must','may','could'],1,'红灯时停车是必须遵守的规则'],
    ['You look tired. You ___ go to bed early.', ['must not','can not','should','may not'],2,'should用于提出合理建议'],
    ['Students ___ run in the hallway because it is dangerous.', ['can','should','must','must not'],3,'危险行为属于明确禁止，用must not'],
    ['“Must I finish today?” “No, you ___.”', ["needn't","mustn't","can't","shouldn't"],0,'must问句的否定回答用need not表示不必'],
  ]},
  { topic: '特殊疑问词与句序', rows: [
    ['___ is your birthday? In May.', ['What','When','Where','Who'],1,'回答月份，询问时间用When'],
    ['___ do you go to school? By bus.', ['Why','What','How','Where'],2,'回答交通方式，询问方式用How'],
    ['___ book is this? It is Mike’s.', ['What','Who','Where','Whose'],3,'询问物品所属用Whose'],
    ['Which question is correct?', ['Where does she live?','Where she does live?','Where lives she?','Where she live does?'],0,'特殊疑问句采用疑问词+助动词+主语+动词原形'],
    ['Could you tell me ___?', ['where is the library','where the library is','the library where is','is where the library'],1,'宾语从句应使用陈述句语序where the library is'],
  ]},
];
export const PRIMARY2_GRAMMAR_QUESTIONS = choiceBank('grammar', grammarLevels);

const vocabularyLevels = [
  { topic: '校园场所与课程', rows: [
    ['We borrow books from the ___.',['library','playground','canteen','office'],0,'借书的地点是library'],['We have PE on the ___.',['classroom','playground','kitchen','bedroom'],1,'体育活动通常在playground进行'],['We learn numbers in ___ class.',['music','art','math','Chinese'],2,'数字计算属于math'],['The room where teachers work is the ___.',['library','lab','gym','office'],3,'教师办公地点是office'],['Science students do experiments in the ___.',['laboratory','hallway','garden','gate'],0,'实验在laboratory中进行']]},
  { topic: '家庭成员与人物特征', rows: [
    ["My father's brother is my ___.",['uncle','aunt','cousin','grandpa'],0,'父亲的兄弟是uncle'],['Lily always helps others. She is ___.',['tall','kind','short','thin'],1,'乐于助人体现kind'],['Tom tells funny stories. He is ___.',['quiet','shy','humorous','angry'],2,'会讲有趣故事说明humorous'],['My baby brother never talks to strangers. He is ___.',['active','brave','outgoing','shy'],3,'不敢和陌生人说话体现shy'],['My sister has ___ hair, not curly hair.',['straight','round','heavy','young'],0,'straight与curly形成反义']]},
  { topic: '食物饮料与量词', rows: [
    ['I would like a ___ of water.',['glass','piece','pair','plate'],0,'water可用a glass of计量'],['Mum cooked a ___ of soup.',['cup','bowl','slice','bar'],1,'soup常用bowl盛放'],['Please give me two ___ of bread.',['bottles','bowls','pieces','cups'],2,'bread不可数，可说pieces of bread'],['We need a ___ of milk.',['piece','pair','plate','carton'],3,'盒装牛奶可说a carton of milk'],['Eating fresh ___ is good for us.',['vegetables','candies','cola','sugar'],0,'fresh vegetables有益健康']]},
  { topic: '衣物颜色与价格', rows: [
    ['We wear a ___ around the neck.',['scarf','glove','sock','cap'],0,'scarf围在脖子上'],['It is cold. Please ___ your coat.',['wear','put on','take off','look at'],1,'put on强调穿上的动作'],['These shoes are too ___. I need a larger pair.',['cheap','pretty','small','clean'],2,'需要大一码说明鞋太small'],['“How much is the shirt?” asks about its ___.',['color','size','style','price'],3,'How much询问price'],['The red dress is ¥80. It is cheaper than the blue one at ¥100. “Cheaper” means ___.',['lower in price','bigger in size','darker in color','older in style'],0,'80元比100元价格更低']]},
  { topic: '天气季节与活动', rows: [
    ['We can make a snowman in ___.',['winter','spring','summer','autumn'],0,'冬季有雪可堆雪人'],['It is ___ today. The sun is bright.',['rainy','sunny','cloudy','snowy'],1,'太阳明亮说明sunny'],['Leaves turn yellow and fall in ___.',['spring','summer','autumn','winter'],2,'秋季树叶变黄飘落'],['A strong ___ can blow away my hat.',['sun','cloud','rain','wind'],3,'wind能吹走帽子'],['It is raining heavily, so the game is ___.',['canceled','planted','painted','invited'],0,'大雨会导致比赛取消']]},
  { topic: '交通与方位', rows: [
    ['I go to school ___ bus.',['by','on','in','with'],0,'交通方式用by bus'],['The bank is ___ the post office and the shop.',['behind','between','across','through'],1,'两个地点之间用between'],['The hospital is ___ from the park.',['under','inside','across','along'],2,'across from表示在对面'],['Walk ___ the street and turn left.',['between','behind','inside','along'],3,'along the street表示沿街走'],['Do not go ___ the road when the light is red.',['across','between','above','around'],0,'穿过道路用across']]},
  { topic: '身体疾病与健康', rows: [
    ['We use our ___ to hear.',['ears','eyes','nose','hands'],0,'耳朵用于听'],['I have a ___. My head hurts.',['cold','headache','toothache','cough'],1,'头疼是headache'],['Brush your ___ twice a day.',['hands','eyes','teeth','hair'],2,'每天刷teeth保持口腔健康'],['You should drink water and get enough ___.',['candy','games','noise','sleep'],3,'充足sleep有益健康'],['Too much candy is bad for your ___.',['teeth','knees','ears','arms'],0,'糖果过量损害牙齿']]},
  { topic: '动物能力与栖息地', rows: [
    ['A fish lives in ___.',['water','trees','grass','snow'],0,'鱼生活在water中'],['A monkey is good at ___.',['swimming deep','climbing trees','flying high','digging roads'],1,'猴子擅长climbing trees'],['Polar bears live in very ___ places.',['hot','dry','cold','noisy'],2,'北极熊栖息地寒冷'],['Camels can travel in the ___.',['forest','ocean','river','desert'],3,'骆驼适应desert'],['We should protect wild animals and their ___.',['homes','toys','lessons','clothes'],0,'保护动物也要保护其栖息地homes']]},
  { topic: '高频动词搭配', rows: [
    ['I ___ my homework after dinner.',['do','make','take','go'],0,'固定搭配do homework'],['Let us ___ a cake for Mum.',['do','make','take','go'],1,'制作蛋糕用make'],['Please ___ a photo of us.',['do','make','take','go'],2,'固定搭配take a photo'],['We often ___ swimming in summer.',['do','make','take','go'],3,'运动搭配go swimming'],['Be careful not to ___ the same mistake again.',['make','do','take','go'],0,'固定搭配make a mistake']]},
];
export const PRIMARY2_VOCABULARY_QUESTIONS = choiceBank('vocabulary', vocabularyLevels);
