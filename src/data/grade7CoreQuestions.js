function balance(options, answer, index) {
  const correct = options[answer];
  const arranged = options.filter((_, optionIndex) => optionIndex !== answer);
  const correctIndex = index % 4;
  arranged.splice(correctIndex, 0, correct);
  return { options: arranged, correctIndex };
}

function buildBank(moduleId, levels, type = 'choice') {
  return levels.map(({ topic, rows }, levelIndex) => ({
    level: levelIndex + 1,
    passScore: 3,
    questions: rows.map(([question, options, answer, reason], questionIndex) => ({
      id: `grade7_${moduleId}_${levelIndex + 1}_${questionIndex + 1}`,
      question,
      ...balance(options, answer, levelIndex * 5 + questionIndex),
      explanation: `本题考查${topic}。${reason}；其余选项不符合语法规则、固定搭配或当前语境。`,
      type,
    })),
  }));
}

const grammarLevels = [
  { topic: '冠词体系', rows: [
    ['There is ___ apple on the desk.',['a','an','the','/'],1,'apple以元音音素开头，用an'],
    ['My father is ___ English teacher.',['a','an','the','/'],1,'职业名词单数且English以元音音素开头'],
    ['We play ___ basketball after school.',['a','an','the','/'],3,'球类运动前通常不用冠词'],
    ['___ sun rises in the east.',['A','An','The','/'],2,'独一无二的事物sun前用the'],
    ['This is ___ useful book.',['a','an','the','/'],0,'useful以辅音音素/j/开头，用a'],
  ]},
  { topic: '可数与不可数名词', rows: [
    ['How many ___ are in the box?',['bread','milk','tomatoes','rice'],2,'how many修饰可数名词复数'],
    ['We need some ___ for dinner.',['potato','egg','rice','apple'],2,'rice是不可数名词'],
    ['There ___ some milk in the glass.',['is','are','be','am'],0,'milk不可数，there be用is'],
    ['Please give me two ___ of bread.',['piece','pieces','glass','cup'],1,'两片面包用two pieces of bread'],
    ['There are three ___ on the table.',['box','boxs','boxes','boxies'],2,'box复数加-es'],
  ]},
  { topic: '代词宾格与所有格', rows: [
    ['Lucy is my friend. I often help ___.',['she','her','hers','herself'],1,'动词help后用宾格her'],
    ['This is not my ruler. ___ is blue.',['My','Mine','Me','I'],1,'mine独立表示my ruler'],
    ['Mr Li teaches ___ English.',['we','our','us','ours'],2,'动词teaches后用宾格us'],
    ['That is ___ classroom.',['they','them','theirs','their'],3,'名词classroom前用形容词性物主代词their'],
    ['These books are ___.',['our','ours','us','we'],1,'空后无名词，用名词性物主代词ours'],
  ]},
  { topic: 'there be句型', rows: [
    ['There ___ a library near my home.',['is','are','have','has'],0,'就近主语a library为单数'],
    ['There ___ two pens and a book in the bag.',['is','are','has','have'],1,'就近主语two pens为复数'],
    ['___ there any water in the bottle?',['Are','Do','Is','Does'],2,'water不可数，疑问句用Is there'],
    ['There is not ___ orange juice.',['many','a few','some','any'],3,'否定句通常用any'],
    ['How many students ___ there in your class?',['are','is','have','has'],0,'students为复数，用are there'],
  ]},
  { topic: '一般现在时', rows: [
    ['Tom usually ___ to school by bike.',['go','goes','going','went'],1,'第三人称单数一般现在时用goes'],
    ['My parents ___ TV after dinner.',['watches','watch','watching','watched'],1,'复数主语用动词原形'],
    ['___ your sister like music?',['Do','Is','Does','Has'],2,'第三人称单数疑问句借助Does'],
    ['Jack does not ___ computer games on weekdays.',['plays','played','playing','play'],3,'does not后用动词原形'],
    ['Water ___ at 100°C.',['boils','boil','boiling','boiled'],0,'客观事实用一般现在时，water作单数'],
  ]},
  { topic: '频率副词', rows: [
    ['I ___ go to bed late because I need enough sleep.',['never','always','usually','often'],0,'需要充足睡眠，所以从不晚睡'],
    ['She is ___ late for school; she arrives on time every day.',['always','never','often','sometimes'],1,'每天准时说明never late'],
    ['We go swimming twice a week. We ___ go swimming.',['never','sometimes','often','always'],2,'每周两次可用often'],
    ['My grandfather takes a walk every evening. He ___ takes a walk.',['never','sometimes','often','always'],3,'每天都做表示always'],
    ['Frequency adverbs usually come ___ the main verb.',['before','after','under','across'],0,'频率副词通常位于实义动词之前'],
  ]},
  { topic: '祈使句', rows: [
    ['___ quietly in the library.',['Speak','Speaks','Speaking','To speak'],0,'肯定祈使句以动词原形开头'],
    ['___ run in the hallway.',['Not','Doesn’t','Don’t','No'],2,'否定祈使句用Don’t加动词原形'],
    ['Please ___ your homework on time.',['finishes','finished','finishing','finish'],3,'please后祈使句用动词原形'],
    ['___ careful when you cross the road.',['Be','Is','Are','Being'],0,'系动词祈使句用Be开头'],
    ['Let us ___ the classroom together.',['cleans','clean','cleaned','cleaning'],1,'let后接动词原形'],
  ]},
  { topic: '现在进行时辨析', rows: [
    ['Look! The children ___ football.',['play','plays','are playing','played'],2,'Look提示动作正在发生'],
    ['Mum ___ dinner in the kitchen now.',['cooks','cooked','cooking','is cooking'],3,'now提示现在进行时，单数主语用is'],
    ['Listen! Someone ___ at the door.',['is knocking','knocks','knocked','knock'],0,'Listen提示当前正在敲门'],
    ['We usually ___ lunch at school.',['are having','have','had','having'],1,'usually提示一般现在时'],
    ['What ___ you ___ at the moment?',['do; do','are; do','are; doing','did; do'],2,'at the moment提示are doing'],
  ]},
  { topic: '句型转换综合', rows: [
    ['Choose the negative form of “She likes apples.”',['She not likes apples.','She doesn’t likes apples.','She isn’t like apples.','She doesn’t like apples.'],3,'第三人称单数否定用doesn’t加动词原形'],
    ['Choose the question for “He goes by bus.”',['How does he go?','How he goes?','What does he goes?','Does how he go?'],0,'询问方式用How does，谓语还原'],
    ['“Those are dictionaries.” The singular form is:',['That are a dictionary.','That is a dictionary.','Those is dictionary.','This are dictionaries.'],1,'those变that，are变is，复数名词变单数'],
    ['Choose the question for “The book is ten yuan.”',['Where is the book?','Whose book is it?','How much is the book?','How many books are there?'],2,'询问价格用How much'],
    ['“There are some trees.” Change it into a question.',['There are any trees?','Do there have trees?','Is there some trees?','Are there any trees?'],3,'there be疑问句提前are，some变any'],
  ]},
];

const vocabularyLevels = [
  { topic: '校园物品', rows: [
    ['Use a ___ to look up a new word.',['dictionary','notebook','eraser','ruler'],0,'dictionary用于查词'],
    ['We do science experiments in the ___.',['library','laboratory','playground','canteen'],1,'laboratory是实验室'],
    ['Write the homework in your ___.',['calculator','uniform','notebook','globe'],2,'notebook用于记录作业'],
    ['A ___ shows countries and oceans.',['stapler','drawer','calendar','globe'],3,'globe是地球仪'],
    ['Borrow books from the ___.',['library','gym','office','gate'],0,'library提供图书借阅'],
  ]},
  { topic: '家庭关系', rows: [
    ['My father’s brother is my ___.',['cousin','uncle','nephew','grandfather'],1,'父亲的兄弟是uncle'],
    ['My aunt’s daughter is my ___.',['sister','niece','cousin','mother'],2,'姨/姑的女儿是cousin'],
    ['My mother’s mother is my ___.',['aunt','sister','cousin','grandmother'],3,'母亲的母亲是grandmother'],
    ['A family with parents and children is a ___ family.',['nuclear','school','single word','sports'],0,'nuclear family指核心家庭'],
    ['My parents’ parents are my ___.',['relatives','grandparents','classmates','neighbors'],1,'父母的父母是grandparents'],
  ]},
  { topic: '食物与营养', rows: [
    ['Carrots and spinach are ___.',['drinks','grains','vegetables','desserts'],2,'胡萝卜和菠菜属于蔬菜'],
    ['Milk and cheese are rich in ___.',['sugar only','salt only','oil','calcium'],3,'乳制品富含钙'],
    ['A ___ diet includes different kinds of healthy food.',['balanced','noisy','expensive','empty'],0,'balanced diet是均衡饮食'],
    ['Too much ___ is bad for your teeth.',['protein','sugar','water','fiber'],1,'过多糖分损害牙齿'],
    ['Beans and eggs can provide ___.',['vitamins only','water only','protein','air'],2,'豆类和鸡蛋提供蛋白质'],
  ]},
  { topic: '体育与爱好', rows: [
    ['You need a racket to play ___.',['football','swimming','running','tennis'],3,'tennis使用球拍'],
    ['People wear goggles when they go ___.',['swimming','cycling','skating','hiking'],0,'游泳常用护目镜'],
    ['My hobby is ___ stamps from different countries.',['collect','collecting','collected','collection'],1,'hobby is后用动名词collecting'],
    ['A person who plays football is a football ___.',['play','playing','player','played'],2,'player表示运动员'],
    ['We use a helmet when we go ___.',['reading','singing','painting','cycling'],3,'骑行戴头盔'],
  ]},
  { topic: '学科与日期', rows: [
    ['We learn about the past in ___ class.',['history','geography','biology','music'],0,'history研究过去'],
    ['Maps and countries belong to ___.',['physics','geography','art','math'],1,'geography学习地图国家等'],
    ['The month after September is ___.',['August','November','October','December'],2,'九月之后是十月'],
    ['The first day of a school week is often ___.',['Friday','Sunday','Saturday','Monday'],3,'学校周通常从Monday开始'],
    ['We study living things in ___.',['biology','history','PE','music'],0,'biology研究生物'],
  ]},
  { topic: '社区地点', rows: [
    ['You can mail a letter at the ___.',['hospital','post office','bank','theater'],1,'post office办理邮寄'],
    ['People see a doctor in a ___.',['museum','bakery','hospital','station'],2,'hospital提供医疗服务'],
    ['You can watch a play at the ___.',['supermarket','pharmacy','crossing','theater'],3,'theater观看戏剧'],
    ['Buy medicine at the ___.',['pharmacy','bookstore','restaurant','hotel'],0,'pharmacy是药店'],
    ['Save or withdraw money at a ___.',['bakery','bank','cinema','park'],1,'bank办理存取款'],
  ]},
  { topic: '日常活动', rows: [
    ['I ___ my bed after getting up.',['do','take','make','go'],2,'固定搭配make the bed'],
    ['Please ___ out the rubbish before dinner.',['make','do','go','take'],3,'固定搭配take out the rubbish'],
    ['Students usually ___ notes in class.',['take','make','go','have'],0,'固定搭配take notes'],
    ['We ___ the dishes after dinner.',['make','do','take','go'],1,'do the dishes表示洗餐具'],
    ['She ___ online to check her email.',['does','takes','goes','makes'],2,'go online表示上网'],
  ]},
  { topic: '动物特征', rows: [
    ['A giraffe has a very long ___.',['tail','wing','beak','neck'],3,'长颈鹿特征是长脖子'],
    ['Birds use their ___ to fly.',['wings','paws','fins','horns'],0,'鸟用翅膀飞行'],
    ['A dolphin is a smart sea ___.',['reptile','mammal','insect','bird'],1,'海豚属于哺乳动物'],
    ['A camel can live in a dry ___.',['ocean','jungle','desert','river'],2,'骆驼适应沙漠'],
    ['Animals active at night are called ___.',['domestic','dangerous','colorful','nocturnal'],3,'nocturnal意为夜间活动的'],
  ]},
  { topic: '旅行与交通', rows: [
    ['Passengers wait for a train on the ___.',['platform','runway','harbor','crossroad'],0,'火车乘客在站台等候'],
    ['You need a ___ before boarding a plane.',['menu','boarding pass','receipt','calendar'],1,'登机需要boarding pass'],
    ['A trip by ship is a ___.',['flight','drive','voyage','walk'],2,'voyage表示海上航行'],
    ['The place where planes take off is an ___.',['underground','station','port','airport'],3,'飞机从airport起飞'],
    ['A ___ tells travelers where and when to go.',['schedule','recipe','uniform','dictionary'],0,'schedule提供行程时间安排'],
  ]},
];

export const GRADE7_GRAMMAR_QUESTIONS = buildBank('grammar', grammarLevels, 'grammar');
export const GRADE7_VOCABULARY_QUESTIONS = buildBank('vocabulary', vocabularyLevels);
