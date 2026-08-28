function balance(options, answer, index) {
  const correct = options[answer];
  const arranged = options.filter((_, optionIndex) => optionIndex !== answer);
  const correctIndex = index % 4;
  arranged.splice(correctIndex, 0, correct);
  return { options: arranged, correctIndex };
}

function buildBank(moduleId, levels, listeningCount) {
  return levels.map(([topic, rows], levelIndex) => ({
    level: levelIndex + 1,
    passScore: 3,
    questions: rows.map(([question, audioText, options, answer], questionIndex) => ({
      id: `grade7_${moduleId}_${levelIndex + 1}_${questionIndex + 1}`,
      question,
      ...(audioText ? { audioText } : {}),
      ...balance(options, answer, levelIndex * 5 + questionIndex),
      explanation: `本题考查${topic}。${options[answer]}符合听到的信息、交际目的和自然表达；其他选项信息错误、答非所问或语气不恰当。`,
      type: questionIndex < listeningCount ? 'listening' : 'choice',
    })),
  }));
}

const speakingLevels = [
  ['介绍朋友', [
    ['What is Ella’s friend good at?','This is my friend Kate. She is friendly and she plays the violin very well.',['Playing the violin','Playing basketball','Drawing maps','Cooking noodles'],0],
    ['How long have the speakers known each other?','We met in Grade Five, so we have been friends for two years.',['One year','Two years','Three years','Five years'],1],
    ['Which introduction includes personality and interest?','',['This is Leo.','Leo is thirteen.','This is Leo. He is patient and loves science.','Leo sits there.'],2],
    ['Your two friends have not met. Say:','',['You are both students.','His name has four letters.','Friends often talk.','Mia, I’d like you to meet Jack.'],3],
    ['How do you continue after introducing a friend?','',['We both enjoy collecting stamps.','Close your notebook.','The bus leaves at six.','How much is this?'],0],
  ]],
  ['谈论课表', [
    ['When does the science lesson begin?','Science begins at ten twenty, after the morning break.',['Ten o’clock','Ten twenty','Eleven twenty','After lunch'],1],
    ['Why does Max like geography?','Geography is my favorite because I enjoy learning about different places.',['It has no homework','He likes learning about places','The teacher gives food','It is always outdoors'],1],
    ['Ask about a subject preference:','',['Where is your book?','When is lunch?','Which subject do you like best?','How many classrooms?'],2],
    ['You have two math lessons on Tuesday. Say:','',['Math is a number.','Tuesday comes second.','I have a ruler.','We have math twice on Tuesday.'],3],
    ['Explain why you dislike a subject politely:','',['History is difficult for me, but I’m working on it.','History is useless.','I never listen in history.','The teacher is wrong.'],0],
  ]],
  ['餐厅点餐', [
    ['What does the customer choose?','I’ll have the beef noodles without onions, please.',['Beef noodles without onions','Chicken rice','Vegetable soup','Beef noodles with onions'],0],
    ['How many drinks are ordered?','Two lemon teas, please—one hot and one cold.',['One','Two','Three','Four'],1],
    ['How do you ask about ingredients?','',['Is the waiter busy?','Where is my chair?','Does this soup contain peanuts?','Who drew the menu?'],2],
    ['The dish is unavailable. A natural reply is:','',['Bring it anyway.','I dislike restaurants.','The menu is paper.','Then I’ll try the fish, please.'],3],
    ['How should a waiter confirm an order?','',['That’s one salad and two soups, correct?','Do you live nearby?','Your food has a color.','Please order yesterday.'],0],
  ]],
  ['约时间', [
    ['When will they meet?','Let’s meet at a quarter past three outside the library.',['3:15','3:30','3:45','4:15'],0],
    ['Why can’t Hana meet on Friday?','Friday doesn’t work because I have basketball practice.',['She has homework','She has basketball practice','She is traveling','The library is closed'],1],
    ['Which question checks availability?','',['What is your hobby?','Where is my watch?','Are you free on Saturday afternoon?','How old is the clock?'],2],
    ['Suggest another time:','',['Time goes quickly.','Saturday is a day.','I have no calendar.','How about Sunday morning instead?'],3],
    ['Confirm the arrangement clearly:','',['Great, see you at the gate at ten.','Maybe somewhere someday.','I know what time means.','Meetings are useful.'],0],
  ]],
  ['问路', [
    ['Where should the visitor turn?','Walk past the bank and turn left at the second crossing.',['At the second crossing','At the bank door','At the first bridge','At the bus stop'],0],
    ['How far is the museum?','It’s about a ten-minute walk from here.',['Five minutes by bus','Ten minutes on foot','Twenty minutes by bike','An hour by car'],1],
    ['Ask for directions politely:','',['Tell me the road.','Museum where?','Excuse me, could you tell me the way to the museum?','You must guide me.'],2],
    ['Which instruction is clearest?','',['Move somewhere.','Follow people.','Find a building.','Go straight for two blocks, then turn right.'],3],
    ['After receiving help, say:','',['Thanks. That’s very helpful.','I knew it already.','Your map is old.','Walk away now.'],0],
  ]],
  ['谈规则', [
    ['What rule does the student mention?','We must wear sports shoes in the gym.',['Wear sports shoes','Bring lunch','Turn off computers','Arrive at noon'],0],
    ['Why can’t students eat there?','Food isn’t allowed because it may damage the library books.',['It smells good','It may damage books','Lunch is free','The tables are new'],1],
    ['Which sentence states an obligation?','',['We may leave early.','We like the rule.','Students must show their ID cards.','The sign is blue.'],2],
    ['Explain a rule with a reason:','',['No phones. That’s all.','Phones are small.','I bought a phone.','Keep phones silent so everyone can study.'],3],
    ['Which is a polite reminder?','',['Please remember to return the equipment.','Give it back now!','You always forget!','Rules are boring.'],0],
  ]],
  ['描述人物', [
    ['Who is being described?','She has curly hair, wears glasses, and carries a red backpack.',['The girl with glasses','The tall boy','The man in black','The child with a hat'],0],
    ['What is Daniel like?','Daniel is quiet at first, but he is kind and always ready to help.',['Noisy and careless','Quiet and helpful','Funny and impatient','Shy and unfriendly'],1],
    ['Which sentence describes appearance?','',['She enjoys music.','She is generous.','She has shoulder-length hair.','She helps everyone.'],2],
    ['Which sentence contrasts two traits?','',['He has a brother.','He wears blue.','He plays chess.','He is serious in class but funny after school.'],3],
    ['Give a respectful description:','',['He is short and energetic.','He looks strange.','Her face is wrong.','She is too fat.'],0],
  ]],
  ['讲述周末', [
    ['What did Alice do first?','On Saturday I cleaned my room, then I met friends at the park.',['Cleaned her room','Met her friends','Watched a film','Visited Grandma'],0],
    ['Why did the picnic move indoors?','It began to rain, so we had our picnic in the community center.',['The food was cold','It began to rain','The park was crowded','Someone was late'],1],
    ['Which sentence begins a past event clearly?','',['Every Sunday is nice.','I may go later.','Last weekend, my family visited a farm.','Weekends have two days.'],2],
    ['Complete the sequence: “First we cycled. ___”','',['Cycling has wheels.','First comes before second.','We like parks.','After that, we stopped for lunch.'],3],
    ['End a weekend story with a feeling:','',['I was tired, but I felt proud.','It happened on Sunday.','There were four people.','The bus was green.'],0],
  ]],
  ['规划旅行', [
    ['Which city are they planning to visit?','We’re thinking of visiting Xi’an during the autumn holiday.',['Xi’an','Chengdu','Beijing','Hangzhou'],0],
    ['How will they travel?','The train takes longer, but we chose it because the station is near our home.',['By plane','By train','By car','By ship'],1],
    ['Which question helps plan accommodation?','',['How old is the city?','Who likes trains?','Where shall we stay?','What did you eat?'],2],
    ['Suggest an itinerary:','',['Trips need bags.','Xi’an is old.','I own a camera.','Let’s visit the museum in the morning and the wall after lunch.'],3],
    ['Compare transport choices:','',['Flying is faster, but the train is cheaper.','Planes have wings.','Tickets are paper.','I traveled last year.'],0],
  ]],
];

const funLevels = [
  ['音标侦探', [
    ['Which word do you hear?','Beach.',['beach','bench','batch','bridge'],0],
    ['Which word contains /eə/?','',['hear','pear','poor','park'],1],
    ['Which pair shares the same vowel sound?','',['food—good','cat—cake','bird—word','home—come'],2],
    ['Which word ends with /ŋ/?','',['thin','think','win','sing'],3],
    ['Which word has a silent “k”?','',['knife','kite','keep','king'],0],
  ]],
  ['姓名文化', [
    ['Which is the family name?','Her full name is Emma Watson. Watson is her family name.',['Watson','Emma','Emma Watson','Ms Emma'],0],
    ['In many English names, the family name comes ___.','',['first','last','in the middle always','before every title'],1],
    ['Which is a common polite form for a teacher named Brown?','',['Teacher First','Brown Teacher','Mr Brown','Brown Man'],2],
    ['In the Chinese name “Li Hua”, Li is usually the ___.','',['given name','nickname','English name','family name'],3],
    ['Which question respects personal choice?','',['What would you like me to call you?','Your name is difficult.','Can I change your name?','Why is your name strange?'],0],
  ]],
  ['校园谜语', [
    ['What place is it?','It is quiet, full of shelves, and you can borrow things to read.',['Library','Gym','Canteen','Playground'],0],
    ['I show dates and hang on a wall. I am a ___.','',['clock','calendar','poster','map'],1],
    ['I remove pencil marks. What am I?','',['ruler','pen','eraser','notebook'],2],
    ['I ring to tell students a lesson starts or ends.','',['Drum','Phone','Radio','Bell'],3],
    ['I have keys and a screen, but I open no door.','',['Computer','Locker','Cupboard','Gate'],0],
  ]],
  ['菜单设计', [
    ['What ingredient should be avoided?','The customer is allergic to peanuts.',['Peanuts','Tomatoes','Rice','Chicken'],0],
    ['Which heading fits juice and milk?','',['Main dishes','Drinks','Desserts','Starters'],1],
    ['Which meal is most balanced?','',['Chips and cola','Cake and candy','Rice, fish and vegetables','Ice cream and juice'],2],
    ['Put a restaurant exchange in order.','',['Pay—Order—Greet','Order—Pay—Greet','Pay—Greet—Order','Greet—Order—Pay'],3],
    ['Which menu note gives useful health information?','',['Contains nuts','Looks nice','Chef likes it','Plate is round'],0],
  ]],
  ['时区挑战', [
    ['What time is it in Tokyo?','It is eight in Beijing and nine in Tokyo.',['9:00','8:00','7:00','10:00'],0],
    ['London is eight hours behind Beijing. Beijing 16:00 is London ___.','',['6:00','8:00','10:00','24:00'],1],
    ['A live lesson starts at 19:00, one hour later than now. Now it is ___.','',['17:00','19:00','18:00','20:00'],2],
    ['Which time is half an hour after 23:45?','',['23:15','00:45','24:75','00:15'],3],
    ['Which tool helps compare world times?','',['A world clock','A ruler','A dictionary','A thermometer'],0],
  ]],
  ['地图寻宝', [
    ['Where is the key?','Walk north to the fountain. The key is under the bench east of it.',['East of the fountain','West of the gate','South of the library','Inside the café'],0],
    ['If the park is north of school, school is ___ of the park.','',['east','south','west','north'],1],
    ['Which route ends at the bank?','',['Left—museum','Straight—park','Right—bank','Back—school'],2],
    ['Order the route from school to the café.','',['Café—Bridge—School','Bridge—School—Café','School—Café—Bridge','School—Bridge—Café'],3],
    ['A compass points to ___.','',['directions','prices','dates','sounds'],0],
  ]],
  ['动物习语', [
    ['Which idiom do you hear?','The classroom was so quiet that you could hear a pin drop.',['Hear a pin drop','A fish out of water','The early bird','Hold your horses'],0],
    ['“A fish out of water” describes someone who feels ___.','',['excited','uncomfortable','hungry','angry'],1],
    ['“The early bird catches the worm” values being ___.','',['strong','quiet','early','colorful'],2],
    ['“Hold your horses” means ___.','',['Ride faster','Feed an animal','Open a gate','Wait patiently'],3],
    ['Which idiom means it rains heavily?','',['Rain cats and dogs','Busy as a bee','Cold fish','Top dog'],0],
  ]],
  ['节日比较', [
    ['Which festival is described?','Families gather, admire the full moon and share mooncakes.',['Mid-Autumn Festival','Thanksgiving','Christmas','Halloween'],0],
    ['Both Spring Festival and Christmas often include ___.','',['dragon boats','family gatherings','pumpkin lanterns','turkey only'],1],
    ['Which food is linked with Thanksgiving?','',['dumplings','mooncakes','turkey','zongzi'],2],
    ['Which comparison is accurate?','',['All festivals have the same date.','Only one culture values family.','Festivals never include food.','Different festivals can share ideas of thanks and reunion.'],3],
    ['Which phrase introduces a difference?','',['In contrast','For example','As a result','First of all'],0],
  ]],
  ['校园广播制作', [
    ['What event is announced?','The school book fair opens in the hall at three this Friday.',['A book fair','A sports match','A music lesson','A class meeting'],0],
    ['Which detail must an announcement include?','',['The speaker’s hobby','Time and place','Every student’s name','A long story'],1],
    ['Which opening suits a school broadcast?','',['Hey, listen!','I have no idea.','Good morning. Here are today’s school notices.','This is probably unimportant.'],2],
    ['Order a clear announcement.','',['Details—Greeting—Event','Event—Closing—Greeting','Closing—Details—Greeting','Greeting—Event—Details'],3],
    ['Which closing gives an action?','',['Please sign up in the office by Thursday.','That was many words.','School is a building.','Maybe something happens.'],0],
  ]],
];

export const GRADE7_SPEAKING_QUESTIONS = buildBank('speaking', speakingLevels, 2);
export const GRADE7_FUN_QUESTIONS = buildBank('fun', funLevels, 1);
