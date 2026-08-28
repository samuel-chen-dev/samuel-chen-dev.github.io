function balanceAnswer(options, answer, target) {
  const correct = options[answer];
  const balanced = options.filter((_, index) => index !== answer);
  balanced.splice(target, 0, correct);
  return { options: balanced, correctIndex: target };
}

function makeChoiceBank(moduleId, levels) {
  return levels.map((level, levelIndex) => ({
    level: levelIndex + 1,
    passScore: 3,
    questions: level.rows.map(([question, options, answer, reason], questionIndex) => {
      const target = (levelIndex * 5 + questionIndex) % 4;
      const correct = options[answer];
      const balanced = options.filter((_, index) => index !== answer);
      balanced.splice(target, 0, correct);
      return {
        id: `primary1_${moduleId}_${levelIndex + 1}_${questionIndex + 1}`,
        question,
        options: balanced,
        correctIndex: target,
        explanation: `本题考查${level.topic}。${reason}；其余选项与题意或基本句型不符。`,
        type: 'choice',
      };
    }),
  }));
}

const grammarLevels = [
  { topic: 'I am句型', rows: [
    ['I ___ Tom.', ['am','is','are','be'],0,'主语I固定搭配am，完整句是I am Tom'],
    ['I ___ a pupil.', ['is','am','are','be'],1,'介绍自己的身份使用I am'],
    ['Hello! ___ Amy.', ['I','I am','Am I','My'],1,'自我介绍应说I am Amy'],
    ['I ___ happy today.', ['are','be','is','am'],3,'I后面的be动词只能用am'],
    ['Which sentence is correct?', ['I is seven.','I are seven.','I am seven.','I be seven.'],2,'表达年龄时正确结构是I am seven'],
  ]},
  { topic: 'this和that', rows: [
    ['___ is my book here.', ['This','That','These','Those'],0,'here表示近处的单个物品，用This'],
    ['___ is a bird over there.', ['This','These','That','They'],2,'over there表示远处的单个事物，用That'],
    ['What is this? ___ a pen.', ['This','It is','That','They are'],1,'回答单个物品用It is'],
    ['This ___ my red bag.', ['am','are','be','is'],3,'This作单数主语，be动词用is'],
    ['Which pair is right?', ['this—near; that—far','this—far; that—near','this—many; that—two','this—people; that—us'],0,'this指近处，that指远处'],
  ]},
  { topic: '不定冠词a和an', rows: [
    ['I see ___ cat.', ['a','an','the cats','two'],0,'cat以辅音音素开头，单数前用a'],
    ['She has ___ apple.', ['a','an','some','two'],1,'apple以元音音素开头，单数前用an'],
    ['It is ___ orange bag.', ['a','two','an','many'],2,'orange以元音音素开头，应使用an'],
    ['He eats ___ banana and ___ egg.', ['an; a','a; a','an; an','a; an'],3,'banana前用a，egg前用an'],
    ['Which sentence is correct?', ['I have an umbrella.','I have a umbrella.','I have an book.','I have a eggs.'],0,'umbrella以元音音素开头，用an'],
  ]},
  { topic: '名词单复数', rows: [
    ['I have one ___.', ['dogs','dog','doges','two dog'],1,'one后接单数名词dog'],
    ['I see two ___.', ['cat','cates','cats','a cat'],2,'two后用复数cats'],
    ['Three ___ are on the desk.', ['box','boxs','a box','boxes'],3,'box的复数加-es，写作boxes'],
    ['One book, two ___.', ['books','book','bookes','a books'],0,'book的规则复数直接加-s'],
    ['Which sentence is correct?', ['There are three bus.','There are three buses.','There is three buses.','There are a buses.'],1,'bus复数为buses，three与复数谓语are搭配'],
  ]},
  { topic: 'have和has', rows: [
    ['I ___ a kite.', ['has','having','have','had'],2,'主语I表示拥有时用have'],
    ['She ___ a doll.', ['have','having','had','has'],3,'主语She是第三人称单数，用has'],
    ['We ___ two balls.', ['have','has','is','are'],0,'主语We搭配have'],
    ['Tom ___ a blue cap.', ['have','has','having','are'],1,'Tom是单个人名，搭配has'],
    ['Which sentence is correct?', ['He have a bike.','They has a bike.','He has a bike.','I has a bike.'],2,'He后应使用has'],
  ]},
  { topic: '情态动词can', rows: [
    ['I can ___.', ['swims','swim','swimming','swam'],1,'can后接动词原形swim'],
    ['A bird can ___.', ['run a book','drink a desk','fly','read wings'],2,'鸟的典型能力是fly'],
    ['Can you dance? Yes, I ___.', ['am','do','dance','can'],3,'Can开头的一般疑问句用can回答'],
    ['The baby ___ drive a car.', ['cannot','can','is','does'],0,'婴儿不会开车，用cannot'],
    ['Which sentence is correct?', ['She can sings.','She can sing.','She cans sing.','She can singing.'],1,'can后必须使用动词原形sing'],
  ]},
  { topic: 'like表达喜好', rows: [
    ['I ___ milk.', ['likes','am','like','has'],2,'主语I表达喜欢用like'],
    ['She ___ cats.', ['like','is like','liking','likes'],3,'She是第三人称单数，用likes'],
    ['Do you like apples? Yes, I ___.', ['do','like','am','can'],0,'Do开头的问句用do作肯定回答'],
    ['Tom does not ___ fish.', ['likes','like','liked','liking'],1,'does not后接动词原形like'],
    ['Which sentence means “我喜欢画画”?', ['I like draw.','I likes drawing.','I like drawing.','I am like drawing.'],2,'like后可接动名词drawing'],
  ]},
  { topic: 'there is句型', rows: [
    ['There ___ a book on the desk.', ['are','am','has','is'],3,'a book是单数，使用There is'],
    ['There is ___ apple in the bag.', ['an','a','two','some'],0,'apple是元音音素开头的单数名词，用an'],
    ['___ is a cat under the chair.', ['This','There','It has','They'],1,'表示某处有某物用There is'],
    ['Is there a park? Yes, there ___.', ['are','has','is','be'],2,'Is there问句用there is回答'],
    ['Which sentence is correct?', ['There are a dog.','There have a dog.','There a dog is.','There is a dog.'],3,'单数存在句正确结构是There is a dog'],
  ]},
  { topic: '基础疑问词', rows: [
    ['___ is your name?', ['What','Where','How many','Who old'],0,'询问名字使用What'],
    ['___ are you? I am seven.', ['What','How old','Where','Who'],1,'询问年龄使用How old'],
    ['___ is my bag? It is on the chair.', ['Who','How','Where','What color'],2,'询问地点使用Where'],
    ['___ is she? She is my sister.', ['What time','How old','Where','Who'],3,'询问人物身份使用Who'],
    ['“它是什么颜色？” should be:', ['What color is it?','Where color is it?','Who color is it?','How old is it?'],0,'询问颜色的固定结构是What color is it'],
  ]},
];

export const PRIMARY1_GRAMMAR_QUESTIONS = makeChoiceBank('grammar', grammarLevels);

const vocabularySets = [
  ['颜色', [['red','红色'],['blue','蓝色'],['yellow','黄色'],['green','绿色'],['black','黑色']]],
  ['数字', [['one','一'],['three','三'],['five','五'],['seven','七'],['ten','十']]],
  ['文具', [['book','书'],['pen','钢笔'],['ruler','尺子'],['eraser','橡皮'],['pencil','铅笔']]],
  ['家庭成员', [['mother','妈妈'],['father','爸爸'],['sister','姐妹'],['brother','兄弟'],['grandma','奶奶或外婆']]],
  ['身体部位', [['eye','眼睛'],['ear','耳朵'],['nose','鼻子'],['hand','手'],['foot','脚']]],
  ['动物', [['cat','猫'],['dog','狗'],['bird','鸟'],['fish','鱼'],['panda','熊猫']]],
  ['食物', [['rice','米饭'],['bread','面包'],['milk','牛奶'],['apple','苹果'],['egg','鸡蛋']]],
  ['天气', [['sunny','晴朗的'],['rainy','下雨的'],['windy','有风的'],['cloudy','多云的'],['snowy','下雪的']]],
  ['日常动作', [['run','跑'],['read','阅读'],['eat','吃'],['sleep','睡觉'],['write','写']]],
];
const vocabularyPool = vocabularySets.flatMap(([, words]) => words.map(([word]) => word));
export const PRIMARY1_VOCABULARY_QUESTIONS = vocabularySets.map(([topic, words], levelIndex) => ({
  level: levelIndex + 1, passScore: 3,
  questions: words.map(([word, meaning], questionIndex) => {
    const target = (levelIndex * 5 + questionIndex) % 4;
    const distractors = vocabularyPool.filter(item => item !== word).slice(levelIndex + questionIndex, levelIndex + questionIndex + 3);
    const options = [...distractors]; options.splice(target, 0, word);
    const prompts = [`Which word means “${meaning}”?`, `Choose the word for “${meaning}”.`, `Find “${meaning}”.`, `What is “${meaning}” in English?`, `Which answer is “${meaning}”?`];
    return { id: `primary1_vocabulary_${levelIndex + 1}_${questionIndex + 1}`, question: prompts[questionIndex], options, correctIndex: target, explanation: `本题考查${topic}词汇。${meaning}的英文是${word}；其他词表示不同的事物或含义。`, type: 'choice' };
  }),
}));

const readingLevels = [
  ['RED APPLE', [
    ['What color is the apple?',['Red','Blue','Green','Black'],0,'标签写着RED'],
    ['What is it?',['A ball','An apple','A cat','A book'],1,'标签中的物品是apple'],
    ['Where may we see this label?',['On a fruit card','On a bus map','On a school bell','On a shoe box'],0,'苹果标签最可能出现在水果卡片上'],
    ['What is the label about?',['A red apple','A blue bag','A green tree','A black cat'],0,'两个词共同描述一个红苹果'],
    ['“Red” is a ___.',['number','color','animal','food'],1,'red在这里表示颜色']]],
  ['A blue bird is in the tree. It can sing.', [
    ['What color is the bird?',['Red','Blue','White','Yellow'],1,'第一句说明鸟是蓝色的'],
    ['Where is the bird?',['In the tree','On a desk','Under a bed','By a bus'],0,'第一句说明鸟在树上'],
    ['What may we hear?',['A bird song','A dog bark','A school bell','A car horn'],0,'鸟会唱歌，因此可能听见鸟鸣'],
    ['What is the text about?',['A singing bird','A running cat','A sleeping dog','A red fish'],0,'全文介绍树上一只会唱歌的鸟'],
    ['“Sing” means ___.',['唱歌','跑步','吃饭','画画'],0,'由bird和声音语境可知sing是唱歌']]],
  ['Hello! I am Ben. I am seven. I like books and football.', [
    ['What is the boy’s name?',['Tom','Ben','Sam','Leo'],1,'自我介绍中说I am Ben'],
    ['How old is Ben?',['Six','Eight','Seven','Nine'],2,'短文说I am seven'],
    ['What may Ben do after school?',['Read or play football','Cook a big meal','Drive a bus','Teach a class'],0,'他的喜好是书和足球'],
    ['What is the text mainly about?',['Ben introduces himself','Ben finds a dog','Ben buys food','Ben sees rain'],0,'短文介绍姓名、年龄和喜好'],
    ['“Like” means ___.',['有','喜欢','看见','需要'],1,'like后接books and football，表示喜欢']]],
  ['This is my family. Dad is tall. Mum has long hair. My baby sister is two. We have a white dog.', [
    ['Who is tall?',['Dad','Mum','The sister','The dog'],0,'短文明确说Dad is tall'],
    ['What color is the dog?',['Brown','Black','Yellow','White'],3,'最后一句说明狗是白色的'],
    ['Who is the youngest?',['Dad','Mum','The baby sister','The dog'],2,'两岁的baby sister是家庭中最年幼的人'],
    ['What is the text about?',['A family','A classroom','A zoo','A shop'],0,'全文介绍家庭成员和宠物'],
    ['“Baby” here means ___.',['年幼的','高大的','白色的','长发的'],0,'baby sister指年幼的妹妹']]],
  ['MONDAY: English, Math, Art. TUESDAY: Chinese, PE, Music. I like Tuesday because I can sing and run.', [
    ['What class is on Monday?',['Music','PE','Art','Chinese'],2,'周一课程表中有Art'],
    ['When does the child have PE?',['Monday','Tuesday','Friday','Sunday'],1,'周二课程含PE'],
    ['Why does the child like Tuesday?',['There is no class','The child can sing and run','The child can sleep all day','There is only Math'],1,'唱歌对应音乐，跑步对应体育'],
    ['What is this text mainly about?',['A class timetable','A food menu','A family card','A weather report'],0,'文本按星期列出课程'],
    ['“PE” is a class for ___.',['sports','cooking','drawing','reading'],0,'PE是体育课']]],
  ['LUNCH MENU: rice ¥2, egg ¥2, milk ¥3, apple ¥3. Amy has ¥5. She wants rice and milk.', [
    ['How much is milk?',['¥2','¥3','¥4','¥5'],1,'菜单标明milk ¥3'],
    ['What does Amy want?',['Egg and apple','Rice and egg','Rice and milk','Milk and apple'],2,'末句说明她想要米饭和牛奶'],
    ['Can Amy pay for them?',['Yes, they cost ¥5','No, they cost ¥6','No, they cost ¥7','Yes, they cost ¥3'],0,'米饭2元加牛奶3元正好5元'],
    ['What is the text mainly for?',['Choosing lunch','Finding a classroom','Calling a friend','Watching weather'],0,'菜单用于选择午餐'],
    ['“Menu” means ___.',['课程表','菜单','地图','卡片'],1,'列出食物和价格的是菜单']]],
  ['SATURDAY WEATHER: Morning—rainy, 15°C. Afternoon—cloudy, 18°C. Take an umbrella.', [
    ['How is the weather in the morning?',['Sunny','Windy','Rainy','Snowy'],2,'天气卡写明Morning—rainy'],
    ['What should you take?',['A ball','An umbrella','A ruler','A kite'],1,'天气卡直接提示带伞'],
    ['When may it be better to play outside?',['In the afternoon','In the rainy morning','At midnight','Before Saturday'],0,'下午转为多云且更暖，较适合户外活动'],
    ['What is the card about?',['Saturday weather','Sunday food','Monday classes','Friday games'],0,'标题是Saturday Weather'],
    ['“Cloudy” means ___.',['晴朗的','多云的','炎热的','下雪的'],1,'下午天气cloudy表示多云']]],
  ['My pet is Coco. She is a small brown dog. Coco likes to run after a red ball. At night, she sleeps by my bed.', [
    ['What animal is Coco?',['A cat','A bird','A dog','A fish'],2,'短文说Coco是dog'],
    ['Where does Coco sleep?',['By the bed','In a tree','On a bus','At school'],0,'末句说她睡在床边'],
    ['What toy should Coco like?',['A red ball','A blue book','A long ruler','A small cup'],0,'她喜欢追红球'],
    ['What is the best title?',['My Pet Coco','My New School','A Rainy Day','A Big Lunch'],0,'全文围绕宠物Coco展开'],
    ['“Pet” means ___.',['野生动物','宠物','玩具','朋友家'],1,'由my dog和日常陪伴可知pet是宠物']]],
  ['Lucy finds a little bird on the road. It cannot fly. She puts it in a box and gives it water. Dad calls an animal helper. Soon, the bird is safe.', [
    ['What does Lucy find?',['A bird','A cat','A ball','A bag'],0,'第一句说她发现一只小鸟'],
    ['Who calls an animal helper?',['Lucy','Mum','Dad','A teacher'],2,'短文说Dad calls an animal helper'],
    ['Why does Lucy put the bird in a box?',['To keep it safe','To make it sing','To take its food','To teach it words'],0,'小鸟不会飞，盒子暂时保护它'],
    ['What is the story mainly about?',['Helping a little bird','Buying a new box','Playing on the road','Learning to fly'],0,'故事讲一家人救助小鸟'],
    ['“Safe” means ___.',['安全的','饥饿的','吵闹的','漂亮的'],0,'得到帮助后小鸟没有危险，safe表示安全']]],
];
export const PRIMARY1_READING_QUESTIONS = readingLevels.map(([passage, rows], levelIndex) => ({
  level: levelIndex + 1, passScore: 3,
  questions: rows.map(([question, options, answer, reason], questionIndex) => ({ id: `primary1_reading_${levelIndex + 1}_${questionIndex + 1}`, passage, question, ...balanceAnswer(options, answer, (levelIndex * 5 + questionIndex) % 4), explanation: `本题考查${questionIndex < 2 ? '细节理解' : questionIndex === 2 ? '简单推理' : questionIndex === 3 ? '主旨理解' : '词义猜测'}。${reason}；其他选项没有短文依据。`, type: 'choice' })),
}));

const clozeLevels = [
  ['Red and blue are {1}. I see a {2} sun and a blue {3}. I like the colors {4} they are bright. They make me {5}.', [['colors','animals','numbers','books'],0,['yellow','small','cold','old'],0,['sky','milk','dog','pen'],0,['but','because','or','if'],1,['happy','rainy','seven','read'],0]],
  ['My schoolbag {1} blue. I have two {2}, a ruler and an eraser {3} it. I take it {4} school every day. It is very {5}.', [['am','is','are','be'],1,['book','books','bookes','a book'],1,['in','on','at','to'],0,['at','to','under','from'],1,['use','useful','using','uses'],1]],
  ['There {1} four people in my family. Dad {2} tall. Mum has long hair, {3} my sister has short hair. We eat dinner {4} home. I love {5}.', [['is','are','am','be'],1,['am','are','is','have'],2,['and','but','or','because'],0,['at','in','on','under'],0,['they','them','their','theirs'],1]],
  ['Mimi is my {1}. She has four white {2}. She can run, {3} she cannot fly. She sleeps {4} the sofa. She is a {5} cat.', [['cat','bird','fish','panda'],0,['leg','legs','leges','a leg'],1,['and','but','because','so'],1,['under','with','from','at'],0,['love','lovely','loving','loves'],1]],
  ['I {1} breakfast at seven. I eat {2} egg and some bread. I drink milk {3} breakfast. Mum says food {4} me grow. Then I feel {5}.', [['has','have','having','had'],1,['a','an','two','many'],1,['for','under','from','with'],0,['help','helps','helping','to help'],1,['energy','energetic','energies','energize'],1]],
  ['Ben {1} to school at eight. He goes {2} bus because school is far. He sits {3} Amy in class. They read {4} write together. Learning is {5}.', [['go','goes','going','went'],1,['by','on','at','in'],0,['near','from','to','over'],0,['but','and','or','because'],1,['fun','rain','hungry','sleep'],0]],
  ['There {1} a park near my home. Children {2} football on the grass. I walk {3} the lake with Dad. We see ducks {4} fish. It is a {5} place.', [['is','are','am','have'],0,['plays','play','playing','played'],1,['by','from','into','at'],0,['but','and','because','so'],1,['peace','peaceful','peacefully','peaces'],1]],
  ['Today {1} my birthday. My friends {2} to my home. We put candles {3} the cake. I make a wish {4} I blow them out. I feel very {5}.', [['am','is','are','be'],1,['come','comes','coming','came'],0,['on','in','under','at'],0,['before','but','or','because'],0,['excite','excited','exciting','excites'],1]],
  ['I {1} up early today. After breakfast, I help Mum {2} the table. Then I play {3} my friend. We read a story {4} go to the park. It is a {5} day.', [['get','gets','getting','got'],0,['clean','cleans','cleaning','cleaned'],0,['with','from','under','at'],0,['and','but','because','or'],0,['wonder','wonderful','wonderfully','wonders'],1]],
];
export const PRIMARY1_CLOZE_QUESTIONS = clozeLevels.map(([template, flatBlanks], levelIndex) => {
  const blanks = Array.from({ length: flatBlanks.length / 2 }, (_, index) => [flatBlanks[index * 2], flatBlanks[index * 2 + 1]]);
  return {
    level: levelIndex + 1,
    passScore: 3,
    questions: blanks.map(([options, answer], questionIndex) => ({
      id: `primary1_cloze_${levelIndex + 1}_${questionIndex + 1}`,
      passage: template.replace(/\{(\d)\}/g, (_, n) => Number(n) === questionIndex + 1 ? '___' : blanks[Number(n) - 1][0][blanks[Number(n) - 1][1]]),
      question: 'Choose the best word for the blank.',
      ...balanceAnswer(options, answer, (levelIndex * 5 + questionIndex) % 4),
      explanation: `本题考查小学启蒙完形语境。${options[answer]}能使句子意思和基本句型正确；其他选项在词义、单复数或搭配上不合适。`,
      type: 'choice',
    })),
  };
});

const speakingLevels = [
  ['问候', [['What should you say?','Good morning!',['Good morning!','Good night!','Goodbye!','Thank you!'],0],['Choose the reply.','How are you?',['I am fine, thank you.','My name is Amy.','I am seven.','It is blue.'],0],['You meet a friend in the afternoon. You say:', '', ['Good morning.','Good afternoon.','Good night.','Goodbye.'],1],['A friend says “Hello!” You reply:', '', ['Hello!','Sorry.','No.','Sleep well.'],0],['Which is a friendly greeting?', '', ['Go away.','Do not talk.','Nice to see you!','Close the door.'],2]]],
  ['姓名', [['What is the girl’s name?','My name is Lucy.',['Lucy','Lily','Amy','Kate'],0],['Choose the best question.','I am Ben.',['How old are you?','What is your name?','Where are you?','How are you?'],1],['“What is your name?” “___”','',['I am eight.','I am fine.','My name is Tom.','It is a book.'],2],['How do you introduce yourself?','',['Your name is Sam.','This is a pen.','Who are you?','Hello, I am Sam.'],3],['You did not hear the name. Say:','',['Sorry, what is your name again?','Your name is wrong.','Do not say it.','I know every name.'],0]]],
  ['年龄', [['How old is the boy?','I am six years old.',['Six','Seven','Eight','Nine'],0],['Choose the question.','She is eight.',['What is it?','How old is she?','Where is she?','Who is she?'],1],['“How old are you?” “___”','',['I am happy.','I am Tom.','I am seven.','I am here.'],2],['Which asks about age?','',['What color?','How many books?','What name?','How old?'],3],['Your friend is nine today. Say:','',['Happy ninth birthday!','Good ninth morning!','Nine books, please.','I am nine, too.'],0]]],
  ['感谢', [['What should you say?','Here is your book.',['Thank you.','I am sorry.','Good night.','No, thanks.'],0],['Choose the reply.','Thank you very much.',['Hello.','You’re welcome.','I am seven.','Good morning.'],1],['A friend helps you. You say:','',['Excuse me.','Goodbye.','Thanks for your help.','What is this?'],2],['Which reply is polite?','',['Give it to me.','I want more.','That is mine.','Thank you. That is very kind.'],3],['You get a gift. You say:','',['Thank you! I love it.','This is too small.','Buy another one.','I do not need you.'],0]]],
  ['请求', [['What does the child ask for?','Can I have some water, please?',['Water','Milk','Bread','Rice'],0],['Choose the reply.','Can you help me, please?',['It is red.','Of course.','I am eight.','Good night.'],1],['You need a pencil. Say:','',['I see a pencil.','Pencils are yellow.','May I use your pencil?','You have two pencils.'],2],['Which request is polite?','',['Open it!','Give me that!','I want it now!','Could you open the door, please?'],3],['You want to go out. Ask:','',['May I go outside, please?','I go outside now.','You must go out.','Outside is green.'],0]]],
  ['喜好', [['What does Mia like?','I like apples.',['Apples','Bananas','Milk','Rice'],0],['Choose the answer.','Do you like cats?',['I am a cat.','Yes, I do.','It is a cat.','Cats can run.'],1],['Ask about a favorite color:','',['Where is red?','Is it a color?','What is your favorite color?','How many colors?'],2],['You do not like milk. Say:','',['Milk is white.','I have milk.','Do you like milk?','I don’t like milk.'],3],['Which answer gives a reason?','',['I like art because I love drawing.','I like art.','Art is on Monday.','This is art.'],0]]],
  ['点餐', [['What does the child order?','I would like some rice, please.',['Rice','Noodles','Bread','Soup'],0],['Choose the reply.','Would you like some milk?',['It is milk.','Yes, please.','I see milk.','Milk is white.'],1],['You are hungry. Say:','',['I am cold.','I am seven.','I would like some food.','I have a ruler.'],2],['Which is a polite order?','',['Give me noodles.','Noodles now.','I want all food.','May I have some noodles, please?'],3],['The waiter brings your food. Say:','',['Thank you.','Good morning.','I am sorry.','What is your name?'],0]]],
  ['天气', [['How is the weather?','It is rainy today.',['Rainy','Sunny','Windy','Snowy'],0],['Choose the advice.','It is cold outside.',['Take a kite.','Wear your coat.','Eat an apple.','Open your book.'],1],['Ask about weather:','',['What day is it?','Where is the sun?','What is the weather like?','How old is it?'],2],['It is sunny. You can say:','',['Take an umbrella.','Wear snow boots.','The rain is heavy.','Let’s play outside.'],3],['Dark clouds come. Say:','',['It may rain. Take an umbrella.','It is a hot lunch.','The cloud is a book.','Let us wear shorts.'],0]]],
  ['告别', [['What should you say?','I am going home now.',['Goodbye!','Good morning!','Thank you!','I am sorry!'],0],['Choose the reply.','See you tomorrow.',['My name is Ben.','See you!','I am seven.','It is Monday.'],1],['Before bed, you say:','',['Good afternoon.','Welcome.','Good night.','Hello.'],2],['Which is a warm goodbye?','',['Go now.','Do not come.','Close it.','Have a nice day!'],3],['A friend leaves for a trip. Say:','',['Have a good trip!','What is your trip?','Trips are long.','Stay at school.'],0]]],
];
export const PRIMARY1_SPEAKING_QUESTIONS = speakingLevels.map(([topic, rows], levelIndex) => ({ level: levelIndex + 1, passScore: 3, questions: rows.map(([question, audioText, options, answer], questionIndex) => ({ id: `primary1_speaking_${levelIndex + 1}_${questionIndex + 1}`, question, ...(audioText ? { audioText } : {}), ...balanceAnswer(options, answer, (levelIndex * 5 + questionIndex) % 4), explanation: `本题考查${topic}情景表达。${options[answer]}能自然、礼貌地回应当前情景；其他选项答非所问或语气不合适。`, type: questionIndex < 2 ? 'listening' : 'choice' })) }));

const funLevels = [
  ['字母形状',[['Which letter has three straight lines?','Look at the capital letter A.',['A','O','C','S'],0],['Which letter is a circle?','',['O','L','T','V'],0],['Which letter looks like a snake?','',['I','X','S','H'],2],['Find the letter after C.','',['A','B','E','D'],3],['Which pair is in ABC order?','',['A, B','B, A','D, C','C, A'],0]]],
  ['字母音',[['Which letter do you hear?','B',['B','D','P','T'],0],['Which word starts with /k/?','',['sun','cat','fish','van'],1],['Which word starts like “ball”?','',['dog','pen','book','cat'],2],['Which word starts with M?','',['nose','lion','kite','moon'],3],['Apple starts with the letter ___.','',['A','E','I','O'],0]]],
  ['押韵词',[['Which word rhymes with cat?','Cat, hat.',['hat','dog','sun','pen'],0],['Which word rhymes with bee?','',['boy','tree','car','book'],1],['Which pair rhymes?','',['fish—book','red—sun','cake—lake','dog—pen'],2],['Which word rhymes with light?','',['late','lot','let','night'],3],['Which pair does NOT rhyme?','',['cat—dog','see—bee','fox—box','day—play'],0]]],
  ['数字歌',[['What number comes next?','One, two, three...',['four','five','six','seven'],0],['Two plus three is ___.','',['four','five','six','seven'],1],['What comes after seven?','',['six','nine','eight','ten'],2],['Ten minus one is ___.','',['seven','eight','ten','nine'],3],['Which number is the biggest?','',['ten','two','five','eight'],0]]],
  ['颜色谜语',[['What color is grass?','Grass is green.',['green','blue','red','black'],0],['Mix red and yellow. What color can you get?','',['green','orange','purple','blue'],1],['I am the color of the sky. I am ___.','',['red','yellow','blue','black'],2],['Pandas are black and ___.','',['red','blue','green','white'],3],['A ripe banana is usually ___.','',['yellow','purple','blue','black'],0]]],
  ['动物叫声',[['Which animal says “meow”?','Meow!',['cat','dog','duck','cow'],0],['Which animal says “woof”?','',['cat','dog','bird','fish'],1],['Which animal says “quack”?','',['cow','pig','duck','sheep'],2],['Which animal says “moo”?','',['dog','cat','duck','cow'],3],['Which animal is usually quiet in water?','',['fish','dog','rooster','cow'],0]]],
  ['节日图标',[['Which festival has a Christmas tree?','Merry Christmas!',['Christmas','Spring Festival','Children’s Day','Mid-Autumn Festival'],0],['We get red envelopes at ___.','',['Christmas','Spring Festival','Halloween','Easter'],1],['Mooncakes are for ___.','',['New Year’s Day','Christmas','Mid-Autumn Festival','Children’s Day'],2],['Children say “Happy Mother’s Day” to ___.','',['teachers','friends','brothers','mothers'],3],['A birthday cake is for a ___.','',['birthday','rainy day','math class','bus ride'],0]]],
  ['情绪语调',[['How does the speaker feel?','Hooray! We won!',['happy','sad','angry','tired'],0],['A child is crying. The child may be ___.','',['happy','sad','hungry only','green'],1],['“Please be quiet!” may sound ___.','',['sleepy','funny','serious','sweet'],2],['You get a surprise gift. You may feel ___.','',['bored','angry','tired','excited'],3],['Your friend is sad. Say:','',['Can I help you?','Go away.','Do not cry.','That is funny.'],0]]],
  ['英语寻宝',[['Find the place.','The key is under the red box.',['Under the red box','On the blue chair','In the green bag','By the door'],0],['The clue says “Go to the place with books.” Go to the ___.','',['kitchen','library','playground','bus stop'],1],['Take the first letter of CAT. It is ___.','',['A','T','C','D'],2],['Put “go / door / the / to” in order.','',['Go door the to.','The go to door.','Door to the go.','Go to the door.'],3],['Final clue: I have hands but cannot clap. I am a ___.','',['clock','book','shoe','tree'],0]]],
];
export const PRIMARY1_FUN_QUESTIONS = funLevels.map(([topic, rows], levelIndex) => ({ level: levelIndex + 1, passScore: 3, questions: rows.map(([question, audioText, options, answer], questionIndex) => ({ id: `primary1_fun_${levelIndex + 1}_${questionIndex + 1}`, question, ...(audioText ? { audioText } : {}), ...balanceAnswer(options, answer, (levelIndex * 5 + questionIndex) % 4), explanation: `本题考查${topic}。${options[answer]}符合听到的内容、词形线索或生活常识；其他选项不符合线索。`, type: questionIndex === 0 ? 'listening' : 'choice' })) }));
