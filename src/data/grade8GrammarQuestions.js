export const GRADE8_GRAMMAR_QUESTIONS = [
  // 第1关：一般现在、过去与将来时的语境统整
  {
    level: 1,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_1_1', question: 'My sister usually ___ her homework before dinner.', options: ['finishes', 'finished', 'will finish', 'is finishing'], correctIndex: 0, explanation: '本题考查一般现在时的时间标志。usually表示经常发生，主语My sister是第三人称单数，所以用finishes；其他选项分别表示过去、将来或正在进行。', type: 'choice' },
      { id: 'grade8_grammar_1_2', question: 'We ___ the science museum last Saturday.', options: ['visit', 'visited', 'will visit', 'are visiting'], correctIndex: 1, explanation: '本题考查一般过去时。last Saturday是明确的过去时间，正确形式是visited；visit用于一般现在时，will visit表示将来，are visiting表示正在进行。', type: 'choice' },
      { id: 'grade8_grammar_1_3', question: 'Look at those dark clouds. It ___ soon.', options: ['was going to rain', 'has started to rain', 'is going to rain', 'would have rained'], correctIndex: 2, explanation: '本题考查有迹象的将来表达。乌云是眼前证据，因此用is going to rain；其他选项分别表示过去的打算、已经开始下雨或与过去事实相反的结果。', type: 'choice' },
      { id: 'grade8_grammar_1_4', question: 'When Lily was young, she ___ to school, but now she ___ the bus.', options: ['walks; takes', 'will walk; took', 'is walking; takes', 'walked; takes'], correctIndex: 3, explanation: '本题考查过去与现在的语境对比。was young要求过去式walked，now表示当前习惯，主语she配takes；其余组合至少有一个时态与时间线不符。', type: 'choice' },
      { id: 'grade8_grammar_1_5', question: 'Our teacher said the earth ___ around the sun.', options: ['moves', 'moved', 'will move', 'was moving'], correctIndex: 0, explanation: '本题考查客观真理不受主句过去时影响。地球绕太阳转是客观事实，所以宾语从句仍用一般现在时moves；其他时态会错误地把真理限制在过去或将来。', type: 'choice' },
    ],
  },
  // 第2关：过去进行时与 when/while
  {
    level: 2,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_2_1', question: 'At eight last night, I ___ a book in my room.', options: ['read', 'was reading', 'will read', 'have read'], correctIndex: 1, explanation: '本题考查过去某时正在进行的动作。at eight last night锁定过去具体时刻，因此用was reading；其余形式不突出该时刻动作正在持续。', type: 'choice' },
      { id: 'grade8_grammar_2_2', question: 'While Tom ___ dinner, the phone rang.', options: ['cooked', 'cooks', 'was cooking', 'has cooked'], correctIndex: 2, explanation: '本题考查while引导的持续背景动作。做饭持续期间电话突然响起，所以用was cooking；cooked不突出持续，cooks和has cooked不符合过去时间线。', type: 'choice' },
      { id: 'grade8_grammar_2_3', question: 'The students were talking quietly when the teacher ___.', options: ['comes in', 'was coming in', 'has come in', 'came in'], correctIndex: 3, explanation: '本题考查长动作与短动作的搭配。were talking是持续动作，老师进来是突然发生的短动作，用came in；其他选项的时态与过去叙事不一致。', type: 'choice' },
      { id: 'grade8_grammar_2_4', question: 'What ___ you ___ when I called you yesterday?', options: ['were; doing', 'did; do', 'are; doing', 'have; done'], correctIndex: 0, explanation: '本题考查过去进行时疑问句。电话打来时询问正在做什么，应使用were you doing；did you do问已完成行为，其他两项不是过去进行时。', type: 'choice' },
      { id: 'grade8_grammar_2_5', question: 'While my parents ___ TV, I was finishing my report.', options: ['watched', 'were watching', 'are watching', 'have watched'], correctIndex: 1, explanation: '本题考查while连接两个同时持续的过去动作。主句用was finishing，从句也应用过去进行时were watching；watched弱化同时持续，另两项时间不匹配。', type: 'choice' },
    ],
  },
  // 第3关：现在完成时 already/yet、since/for
  {
    level: 3,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_3_1', question: 'Jenny has ___ finished her art project.', options: ['yet', 'since', 'already', 'for'], correctIndex: 2, explanation: '本题考查already在现在完成时中的用法。肯定句表示“已经完成”用already；yet常用于疑问或否定句，since和for后面需要接时间。', type: 'choice' },
      { id: 'grade8_grammar_3_2', question: 'Have you seen the new film ___?', options: ['already', 'since', 'for', 'yet'], correctIndex: 3, explanation: '本题考查yet用于现在完成时疑问句。yet通常置于疑问句句末，表示“已经”；already虽可用于疑问句但语气不同，since和for表示时间起点或时长。', type: 'choice' },
      { id: 'grade8_grammar_3_3', question: 'Mr. Green has taught at this school ___ 2018.', options: ['since', 'for', 'during', 'from'], correctIndex: 0, explanation: '本题考查since与时间起点连用。2018是动作开始的具体时间点，所以用since；for接一段时间，during接某段时期，from通常还需与to搭配。', type: 'choice' },
      { id: 'grade8_grammar_3_4', question: 'We ___ each other for nearly five years.', options: ['knew', 'have known', 'are knowing', 'know'], correctIndex: 1, explanation: '本题考查延续性状态与for的搭配。认识持续至今，应使用现在完成时have known；knew只表示过去，know不体现持续到现在，know一般不用进行时。', type: 'choice' },
      { id: 'grade8_grammar_3_5', question: 'Jack ___ the reading club since he entered middle school.', options: ['joined', 'has joined', 'has been in', 'was in'], correctIndex: 2, explanation: '本题考查短暂性动词与持续时间的转换。join不能与since表示的持续时间直接连用，应改为延续性状态has been in；其余选项不能正确表达从过去持续到现在。', type: 'choice' },
    ],
  },
  // 第4关：比较级、最高级及范围表达
  {
    level: 4,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_4_1', question: 'This exercise is ___ than the one we did yesterday.', options: ['difficult', 'most difficult', 'more difficulty', 'more difficult'], correctIndex: 3, explanation: '本题考查多音节形容词比较级。句中than要求比较级，difficult的比较级是more difficult；其他选项或缺少比较级，或词性、结构错误。', type: 'choice' },
      { id: 'grade8_grammar_4_2', question: 'Of the three rivers, the Yellow River is ___.', options: ['the second longest', 'the two longest', 'second longer', 'the longest second'], correctIndex: 0, explanation: '本题考查范围内的序数最高级。of the three给出三者范围，“第二长”表达为the second longest；其他选项分别误用数量、比较级或错误词序。', type: 'choice' },
      { id: 'grade8_grammar_4_3', question: 'The more carefully you plan, the ___ mistakes you will make.', options: ['few', 'fewer', 'fewest', 'less'], correctIndex: 1, explanation: '本题考查“the+比较级，the+比较级”结构。mistakes是可数名词复数，应使用fewer；less修饰不可数名词，few和fewest不符合平行比较结构。', type: 'choice' },
      { id: 'grade8_grammar_4_4', question: 'The new library is twice as ___ as the old one.', options: ['larger', 'largest', 'large', 'largely'], correctIndex: 2, explanation: '本题考查倍数表达“倍数+as+原级+as”。因此形容词使用原级large；larger和largest破坏该结构，largely是副词。', type: 'choice' },
      { id: 'grade8_grammar_4_5', question: 'Shanghai is one of ___ cities in China.', options: ['busier', 'the busier', 'busiest', 'the busiest'], correctIndex: 3, explanation: '本题考查“one of the+最高级+复数名词”。正确表达是one of the busiest cities；其他选项缺少the或未使用正确最高级形式。', type: 'choice' },
    ],
  },
  // 第5关：动词不定式与动名词作宾语
  {
    level: 5,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_5_1', question: 'My cousin hopes ___ a doctor one day.', options: ['to become', 'becoming', 'become', 'became'], correctIndex: 0, explanation: '本题考查hope后接动词不定式。hope to do something是固定结构，所以用to become；其余形式不能直接作hope的宾语。', type: 'choice' },
      { id: 'grade8_grammar_5_2', question: 'Would you mind ___ the window? It is a little cold.', options: ['to close', 'closing', 'close', 'closed'], correctIndex: 1, explanation: '本题考查mind后接动名词。mind doing something表示“介意做某事”，所以选closing；不定式、原形和过去式都不符合该搭配。', type: 'choice' },
      { id: 'grade8_grammar_5_3', question: 'The teacher asked us ___ loudly in the library.', options: ['not talk', 'not talking', 'not to talk', 'to not talking'], correctIndex: 2, explanation: '本题考查ask somebody not to do something。否定词not放在不定式to之前，因此用not to talk；其他选项均不符合宾补结构。', type: 'choice' },
      { id: 'grade8_grammar_5_4', question: 'After two hours, they stopped ___ a short rest.', options: ['taking', 'take', 'taken', 'to take'], correctIndex: 3, explanation: '本题考查stop to do与stop doing的区别。句意是停下原来的活动去休息，应使用stopped to take；stopped taking表示停止休息，含义相反。', type: 'choice' },
      { id: 'grade8_grammar_5_5', question: 'I remember ___ the door, but I cannot remember where I put the key.', options: ['locking', 'to lock', 'lock', 'locked'], correctIndex: 0, explanation: '本题考查remember doing与remember to do的区别。记得已经锁过门要用remember locking；remember to lock表示记得去锁但不说明动作已经完成。', type: 'choice' },
    ],
  },
  // 第6关：情态动词推测、义务与被动语态入门
  {
    level: 6,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_6_1', question: 'The lights are on. Mr. Lee ___ be in his office.', options: ['can’t', 'must', 'needn’t', 'shouldn’t'], correctIndex: 1, explanation: '本题考查情态动词表示肯定推测。灯亮着提供了较强证据，所以用must表示“一定”；can’t是否定推测，needn’t和shouldn’t分别表示不必和不应该。', type: 'choice' },
      { id: 'grade8_grammar_6_2', question: 'This notebook ___ belong to Amy; her name is on the cover.', options: ['might not', 'can’t', 'must', 'needn’t'], correctIndex: 2, explanation: '本题考查有明确证据时的肯定推测。封面有Amy的名字，因此用must belong to；might not和can’t是否定判断，needn’t不表示所属推测。', type: 'choice' },
      { id: 'grade8_grammar_6_3', question: 'You ___ feed the animals in the zoo. It is against the rules.', options: ['must', 'could', 'need', 'mustn’t'], correctIndex: 3, explanation: '本题考查禁止性义务。against the rules说明绝对禁止，所以用mustn’t；must表示必须，could表示可能或许可，need不符合句意。', type: 'choice' },
      { id: 'grade8_grammar_6_4', question: 'School uniforms ___ on Mondays at our school.', options: ['must be worn', 'must wear', 'must be wearing', 'must worn'], correctIndex: 0, explanation: '本题考查情态动词的被动语态。校服是“被穿”，结构应为must be+过去分词，即must be worn；其他选项缺少被动结构或形式错误。', type: 'choice' },
      { id: 'grade8_grammar_6_5', question: 'The broken computer ___ by students themselves; a technician is needed.', options: ['can repair', 'cannot be repaired', 'must repair', 'should be repairing'], correctIndex: 1, explanation: '本题考查情态动词否定被动语态。电脑不能由学生自行修理，应使用cannot be repaired；其余选项把电脑误作动作执行者或使用了错误结构。', type: 'choice' },
    ],
  },
  // 第7关：if条件状语从句与主将从现
  {
    level: 7,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_7_1', question: 'If it ___ tomorrow, we will stay at home.', options: ['will rain', 'rained', 'rains', 'is raining'], correctIndex: 2, explanation: '本题考查真实条件句的主将从现。主句用will stay，if从句用一般现在时rains表示将来；从句通常不用will，其他时态也不合语境。', type: 'choice' },
      { id: 'grade8_grammar_7_2', question: 'You will miss the bus if you ___ now.', options: ['won’t leave', 'left', 'will leave', 'don’t leave'], correctIndex: 3, explanation: '本题考查if条件句的否定形式。主句为一般将来时，从句应用一般现在时don’t leave；if从句不用won’t，left和will leave均不符合主将从现。', type: 'choice' },
      { id: 'grade8_grammar_7_3', question: 'If Tina has enough time, she ___ us with the project.', options: ['will help', 'helps', 'helped', 'would help'], correctIndex: 0, explanation: '本题考查真实条件句主句形式。if从句用一般现在时has，主句应用will help；helps缺少将来意义，helped和would help不符合真实将来条件。', type: 'choice' },
      { id: 'grade8_grammar_7_4', question: 'Unless you ___ your password, your account will remain safe.', options: ['will share', 'share', 'shared', 'are sharing'], correctIndex: 1, explanation: '本题考查unless引导条件状语从句。unless等于if...not，从句遵循主将从现，所以用share；will share不用于该从句，另两项时态不当。', type: 'choice' },
      { id: 'grade8_grammar_7_5', question: 'If everyone ___ a small action, our neighborhood ___ cleaner.', options: ['will take; becomes', 'took; will become', 'takes; will become', 'takes; becomes'], correctIndex: 2, explanation: '本题考查条件句中两个分句的时态配合。if从句主语everyone是第三人称单数，用takes，主句用will become；其他组合违反主将从现或主谓一致。', type: 'choice' },
    ],
  },
  // 第8关：宾语从句的语序、时态与连接词
  {
    level: 8,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_8_1', question: 'Could you tell me ___?', options: ['where is the library', 'where the library was', 'the library is where', 'where the library is'], correctIndex: 3, explanation: '本题考查宾语从句的陈述语序。正确顺序是连接词where+主语the library+谓语is；其他选项使用疑问语序、错误时态或错误词序。', type: 'choice' },
      { id: 'grade8_grammar_8_2', question: 'I wonder ___ the school talent show will begin.', options: ['when', 'that', 'what', 'which'], correctIndex: 0, explanation: '本题考查宾语从句连接词。句中缺少表示时间的状语，因此用when；that不充当成分，what和which不能表达“何时”。', type: 'choice' },
      { id: 'grade8_grammar_8_3', question: 'Do you know ___ David finished the task on time?', options: ['what', 'whether', 'where', 'which'], correctIndex: 1, explanation: '本题考查一般疑问意义的宾语从句。询问David是否按时完成，应用whether；其他连接词分别询问事物、地点或选择范围。', type: 'choice' },
      { id: 'grade8_grammar_8_4', question: 'Lucy said that she ___ tired after the long journey.', options: ['is', 'will be', 'was', 'has been'], correctIndex: 2, explanation: '本题考查主句为过去时时宾语从句的时态呼应。said表示过去，从句描述当时状态，因此用was；其他选项没有与过去叙事一致。', type: 'choice' },
      { id: 'grade8_grammar_8_5', question: 'Our science teacher told us that water ___ at 100°C.', options: ['boiled', 'was boiling', 'would boil', 'boils'], correctIndex: 3, explanation: '本题考查宾语从句表达客观事实。水在100°C沸腾是科学真理，即使主句是过去时仍用一般现在时boils；其他时态会误改事实属性。', type: 'choice' },
    ],
  },
  // 第9关：主谓一致与代词、数量表达综合
  {
    level: 9,
    passScore: 3,
    questions: [
      { id: 'grade8_grammar_9_1', question: 'Everyone in our class ___ ready for the sports meeting.', options: ['is', 'are', 'were', 'be'], correctIndex: 0, explanation: '本题考查不定代词作主语时的主谓一致。everyone按单数处理，所以用is；are和were与单数主语不一致，be不能直接作谓语。', type: 'choice' },
      { id: 'grade8_grammar_9_2', question: 'Neither the teacher nor the students ___ late today.', options: ['was', 'were', 'is', 'be'], correctIndex: 1, explanation: '本题考查neither...nor的就近一致原则。谓语靠近复数主语students，且语境为过去，因此用were；was和is在人称或时态上不符。', type: 'choice' },
      { id: 'grade8_grammar_9_3', question: 'A number of students ___ interested in the robotics club.', options: ['is', 'was', 'are', 'has'], correctIndex: 2, explanation: '本题考查a number of与复数谓语的搭配。a number of表示“许多”，中心意义是复数students，因此用are；其余选项均为单数形式。', type: 'choice' },
      { id: 'grade8_grammar_9_4', question: 'The number of books in our class library ___ growing every year.', options: ['are', 'have', 'were', 'is'], correctIndex: 3, explanation: '本题考查the number of作主语的主谓一致。该短语表示“……的数量”，中心词number是单数，所以用is；其他选项误受books影响。', type: 'choice' },
      { id: 'grade8_grammar_9_5', question: 'Each of the two plans has ___ own advantage, but neither of them ___ perfect.', options: ['its; is', 'their; are', 'its; are', 'their; is'], correctIndex: 0, explanation: '本题综合考查代词指代与主谓一致。each和neither都按单数处理，因此用its和is；其余选项使用复数代词或复数谓语，造成一致关系错误。', type: 'choice' },
    ],
  },
];
