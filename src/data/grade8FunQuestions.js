const LEVELS = [
  ['重音与同形异音词', [
    ['Listen. Which word is stressed on the first syllable?', 'Please keep a record of your reading.', ['record (noun)','record (verb)','present (verb)','permit (verb)'],0,'名词record重音在第一音节'],
    ['Which word is stressed on the second syllable?', '', ['a present','a record','to present','a project'],2,'动词present重音落在第二音节'],
    ['In “They object to the plan,” object is a ___.', '', ['noun','adjective','adverb','verb'],3,'object表示反对，是动词且重音后移'],
    ['Which pair changes stress when its word class changes?', '', ['record / record','teacher / teacher','window / window','happy / happy'],0,'record作名词和动词时重音位置不同'],
    ['“The permit will permit entry.” Which statement is correct?', '', ['Both have final stress','The noun has first stress; the verb has final stress','Both have first stress','Neither word has stress'],1,'名词permit前重音，动词permit后重音'],
  ]],
  ['缩略语与数字交流', [
    ['Listen. What does “DIY” mean?', 'This shelf is a DIY project.', ['Do It Yourself','Drive It Yesterday','Draw It Yellow','Design Inside Yard'],0,'DIY是Do It Yourself的首字母缩写'],
    ['“ASAP” asks someone to act ___.', '', ['after school','as soon as possible','at a safe place','as simply as planned'],1,'ASAP表示尽快'],
    ['Which abbreviation is suitable in an informal message?', '', ['BTW, thanks for your help.','BTW in a formal law','LOL in a school certificate','IDK in an exam answer'],0,'BTW常用于非正式消息，其他场景语域不当'],
    ['What does “FAQ” usually contain?', '', ['Private photos','Final art quality','Fast answer quiz','Frequently asked questions'],3,'FAQ是常见问题集'],
    ['A teacher writes “e.g.” before examples. It means ___.', '', ['for example','that is exactly','and so on','compare with'],0,'e.g.来自拉丁语，表示例如'],
  ]],
  ['英美校园文化', [
    ['Listen. Which place is being described?', 'Students eat lunch together in the cafeteria.', ['Dining area','Science lab','Sports field','Music room'],0,'cafeteria是学生集中用餐区域'],
    ['In many British schools, “head teacher” is closest to ___.', '', ['class monitor','school principal','sports coach','school guard'],1,'head teacher相当于校长'],
    ['Which word is mainly British English?', '', ['vacation','soccer','holiday','elevator'],2,'holiday在英式英语中常指假期'],
    ['A US student says “I am a freshman.” The student is most likely ___.', '', ['a first-year student','a final-year teacher','a school visitor','a club leader'],0,'freshman通常指一年级新生'],
    ['What is the best attitude toward school differences?', '', ['One system is always better','Learn the context before judging','Copy every foreign custom','Avoid discussing differences'],1,'先理解文化背景再评价体现跨文化意识'],
  ]],
  ['英语习语', [
    ['Listen. What does the speaker mean?', 'The quiz was a piece of cake.', ['It was very easy','It tasted sweet','It was too small','It needed cooking'],0,'a piece of cake表示非常容易'],
    ['“Break the ice” means to ___.', '', ['damage some ice','start friendly conversation','end a friendship','feel extremely cold'],1,'该习语表示打破初见时的拘谨'],
    ['If someone “hits the books,” they ___.', '', ['throw textbooks','buy new books','study hard','write a novel'],2,'hit the books表示努力学习'],
    ['“The ball is in your court” means ___.', '', ['you should make the next decision','you must play tennis','the game has ended','someone lost a ball'],0,'该习语表示轮到你决定或行动'],
    ['Which situation is “a blessing in disguise”?', '', ['Missing a bus but meeting an old friend on the next one','Winning after daily practice','Buying exactly what you planned','Sleeping through an important test'],0,'表面不顺却带来意外好结果符合该习语'],
  ]],
  ['条件句逻辑谜题', [
    ['Listen. Which conclusion follows?', 'If it rains, the match is canceled. It is raining.', ['The match is canceled','The match certainly continues','It never rains','The rule is unknown'],0,'条件成立且前件发生，可推出比赛取消'],
    ['If Ava studies, she passes. Ava did not pass. What follows?', '', ['She studied twice','She did not study','She passed later','Nothing can follow'],1,'根据逆否关系可推出她没有学习'],
    ['Only one box has a key. Box A says “Not here.” Box B says “In A.” If B is true, where is it?', '', ['In Box B','In neither box','In Box A','In both boxes'],2,'B陈述为真就直接确定钥匙在A'],
    ['If both signs are false: A says “Key in A”; B says “Key not in B.” Where is the key?', '', ['In Box A','In both','Nowhere','In Box B'],3,'A假说明不在A，B假说明在B'],
    ['Rule: Unless a card is blue, it is round. A card is not round. It must be ___.', '', ['blue','round','red','large'],0,'unless规则的逆否推理得到非圆则必为蓝色'],
  ]],
  ['新闻标题还原', [
    ['Listen. Choose the full sentence.', 'Local team wins city final.', ['The local team won the city final.','The city final wins a team.','A local final was a city.','The team may never win.'],0,'新闻标题省略冠词并常用现在时，完整句恢复冠词和过去时'],
    ['“New Library to Open Friday” means the library ___.', '', ['opened last Friday','will open on Friday','opens every Friday','is closed Friday'],1,'标题中的to open表示计划中的将来'],
    ['“Road Closed After Heavy Rain” uses “closed” as ___.', '', ['an action by the road','a place name','a past participle showing state','a future command'],2,'closed为过去分词，说明道路被关闭的状态'],
    ['Best full form of “Students Raise Money for Shelter”:', '', ['Students raised money for a shelter.','Money raises shelter students.','A shelter raised the students.','Students were money for shelter.'],0,'恢复句子后主谓宾和for目的关系完整'],
    ['Why do headlines omit small words?', '', ['To make them shorter and stronger','Because grammar never matters','To hide the main event','Because readers dislike verbs'],0,'标题省略部分冠词和助动词以求简洁有力'],
  ]],
  ['语调与潜台词', [
    ['Listen. What does the speaker probably feel?', 'Oh, great. Another two hours of homework.', ['Annoyed','Delighted','Relaxed','Proud'],0,'字面great与后句负担形成反讽，语气表达不满'],
    ['A rising tone in “Really?” often shows ___.', '', ['a final command','surprise or a request for confirmation','a prepared speech','complete certainty'],1,'升调常表示惊讶或需要确认'],
    ['“That was brave,” said warmly after a rescue. The speaker is ___.', '', ['complaining','warning','praising','refusing'],2,'温暖语气和勇敢评价共同表达赞扬'],
    ['“Could you possibly turn the music down?” is an example of ___.', '', ['a polite indirect request','an angry direct order','a factual report','an invitation to dance'],0,'could和possibly弱化语气，构成礼貌请求'],
    ['Someone says “Interesting...” and pauses after hearing a doubtful excuse. The subtext may be:', '', ['I am not fully convinced.','I certainly believe every word.','I did not hear anything.','Please repeat the weather report.'],0,'停顿和语调可暗示说话者并未完全相信'],
  ]],
  ['用英语讲中国文化', [
    ['Listen. Which festival is described?', 'Families admire the full moon and share mooncakes.', ['Mid-Autumn Festival','Dragon Boat Festival','Spring Festival','Qingming Festival'],0,'赏月和月饼是中秋节代表活动'],
    ['A “red envelope” is usually given to express ___.', '', ['anger','good wishes','a warning','a school rule'],1,'红包通常承载祝福'],
    ['Which explanation of paper cutting is clearest?', '', ['It is red paper.','People use scissors.','It is a folk art creating patterns from paper.','It is found near windows.'],2,'该句同时交代类别、材料和创作方式'],
    ['How should “the Great Wall” be introduced accurately?', '', ['A historic defensive structure in northern China','The longest modern road in every country','A wall built in one single year','A building used only for sports'],0,'长城是中国北方历史防御工程'],
    ['Best principle when explaining culture in English:', '', ['Translate every word literally','Explain meaning and context clearly','Remove all Chinese features','Use rare words whenever possible'],1,'清楚说明内涵和语境比逐字直译更准确'],
  ]],
  ['密室综合线索', [
    ['Listen. Which object should you inspect?', 'Find the object that has hands but cannot clap.', ['A clock','A mirror','A candle','A ladder'],0,'clock有指针hands但不能拍手'],
    ['The note says “Take the first letter of NORTH.” The code begins with ___.', '', ['T','N','H','R'],1,'NORTH首字母为N'],
    ['If RED=18-5-4, BLUE begins with ___.', '', ['1','2','3','4'],1,'字母B在字母表中序号为2'],
    ['Put the clues in logical order: unlock box, find key, read map.', '', ['Unlock-read-find','Read-unlock-find','Read-find-unlock','Find-unlock-read'],2,'先读地图定位钥匙，再找到钥匙并开箱'],
    ['Final clue: “I speak without a mouth and return your words.” It is ___.', '', ['an echo','a shadow','a footprint','a compass'],0,'echo没有嘴却会返回声音，符合全部线索'],
  ]],
];

function balanceOptions(options, correctIndex, targetIndex) {
  const correct = options[correctIndex];
  const balanced = options.filter((_, index) => index !== correctIndex);
  balanced.splice(targetIndex, 0, correct);
  return balanced;
}

export const GRADE8_FUN_QUESTIONS = LEVELS.map(([topic, rows], levelIndex) => ({
  level: levelIndex + 1, passScore: 3,
  questions: rows.map(([question, audioText, options, correctIndex, reason], questionIndex) => ({
    id: `grade8_fun_${levelIndex + 1}_${questionIndex + 1}`, question, ...(audioText ? { audioText } : {}),
    options: balanceOptions(options, correctIndex, (levelIndex * 5 + questionIndex) % 4),
    correctIndex: (levelIndex * 5 + questionIndex) % 4,
    explanation: `本题考查${topic}。${reason}；其他选项与语言线索、文化背景或逻辑条件不符。`,
    type: questionIndex === 0 ? 'listening' : 'choice',
  })),
}));
