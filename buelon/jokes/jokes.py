import time
import random


jokes = [
    # # {
    # #   "id": 1,
    # #   "setup": "Knock knock!",
    # #   "punchline": "Who's there? Boo! Boo who? Don't cry, it's just a joke!"
    # # },
    # # {
    # #   "id": 2,
    # #   "setup": "What do ghosts serve for dessert?",
    # #   "punchline": "I scream!"
    # # },
    # # {
    # #   "id": 3,
    # #   "setup": "Why didn't the skeleton go to the dance?",
    # #   "punchline": "He had no-body to go with!"
    # # },
    # {
    # "id": 4,
    # "setup": "What's a ghost's favorite fruit?",
    # "punchline": "Boo-berries!"
    # },
    # {
    # "id": 5,
    # "setup": "What do you call a ghost's mistake?",
    # "punchline": "A boo-boo!"
    # },
    # # {
    # #   "id": 6,
    # #   "setup": "What kind of streets do ghosts haunt?",
    # #   "punchline": "Dead ends!"
    # # },
    # # {
    # #   "id": 7,
    # #   "setup": "Why do ghosts like to ride elevators?",
    # #   "punchline": "It lifts their spirits!"
    # # },
    # # {
    # #   "id": 8,
    # #   "setup": "What's a ghost's favorite dessert?",
    # #   "punchline": "Boo-berry pie!"
    # # },
    # # {
    # #   "id": 9,
    # #   "setup": "Why did the ghost go to the bar?",
    # #   "punchline": "For the boos!"
    # # },
    # # {
    # #   "id": 10,
    # #   "setup": "How do ghosts search the web?",
    # #   "punchline": "They use Boo-gle!"
    # # },
    # # {
    # #   "id": 11,
    # #   "setup": "What do you call a ghost's favorite candy?",
    # #   "punchline": "Boo-ble gum!"
    # # },
    # # {
    # #   "id": 12,
    # #   "setup": "Where do baby ghosts go during the day?",
    # #   "punchline": "Dayscare centers!"
    # # },
    # # {
    # #   "id": 13,
    # #   "setup": "What's a ghost's favorite play?",
    # #   "punchline": "Romeo and Boo-liet!"
    # # },
    # # {
    # #   "id": 14,
    # #   "setup": "What do you call a scary ghost?",
    # #   "punchline": "Bam-boo!"
    # # },
    # # {
    # #   "id": 15,
    # #   "setup": "What kind of makeup do ghosts wear?",
    # #   "punchline": "Mas-scare-a!"
    # # },
    # # {
    # #   "id": 16,
    # #   "setup": "What's a ghost's favorite social media app?",
    # #   "punchline": "Snap-BOO!"
    # # },
    # # {
    # #   "id": 17,
    # #   "setup": "What kind of exercise do ghosts enjoy?",
    # #   "punchline": "Boo-camp!"
    # # },
    # # {
    # #   "id": 18,
    # #   "setup": "Why do ghosts make terrible liars?",
    # #   "punchline": "You can see right through them!"
    # # },
    # # {
    # #   "id": 19,
    # #   "setup": "What's a ghost's favorite ice cream flavor?",
    # #   "punchline": "Boo-nilla!"
    # # },
    # # {
    # #   "id": 20,
    # #   "setup": "Why didn't the ghost join the football team?",
    # #   "punchline": "They were afraid he'd phantom the plays!"
    # # },
    # # {
    # #   "id": 21,
    # #   "setup": "How do ghosts cheer for their favorite teams?",
    # #   "punchline": "They stand up and BOO!"
    # # },
    # # {
    # #   "id": 22,
    # #   "setup": "What's a ghost's favorite nursery rhyme?",
    # #   "punchline": "Little Boo Peep!"
    # # },
    # # {
    # #   "id": 23,
    # #   "setup": "What do you call a ghost detective?",
    # #   "punchline": "A boo-sleuth!"
    # # },
    # # {
    # #   "id": 24,
    # #   "setup": "What kind of roads do ghosts travel on?",
    # #   "punchline": "Boo-levards!"
    # # },
    # # {
    # #   "id": 25,
    # #   "setup": "What do you call a ghost's favorite vacation spot?",
    # #   "punchline": "The Boo-hamas!"
    # # },
    # # {
    # #   "id": 26,
    # #   "setup": "What's a ghost's favorite carnival ride?",
    # #   "punchline": "The roller-ghoster!"
    # # },
    # # {
    # #   "id": 27,
    # #   "setup": "Why do ghosts hate the rain?",
    # #   "punchline": "It dampens their spirits!"
    # # },
    # # {
    # #   "id": 28,
    # #   "setup": "What's a ghost's favorite board game?",
    # #   "punchline": "Hide and Shriek!"
    # # },
    # # {
    # #   "id": 29,
    # #   "setup": "What's a ghost's favorite music?",
    # #   "punchline": "Sheet music!"
    # # },
    # # {
    # #   "id": 30,
    # #   "setup": "What kind of ghost haunts gyms?",
    # #   "punchline": "Exorcise ghosts!"
    # # },
    # # {
    # #   "id": 31,
    # #   "setup": "Why did the ghost apply for a loan?",
    # #   "punchline": "He needed some boo-llion!"
    # # },
    # # {
    # #   "id": 32,
    # #   "setup": "What's a ghost's favorite instrument?",
    # #   "punchline": "The boo-kulele!"
    # # },
    # # {
    # #   "id": 33,
    # #   "setup": "What do you call a ghost that gets things wrong?",
    # #   "punchline": "A boo-boo!"
    # # },
    # # {
    # #   "id": 34,
    # #   "setup": "Why did the ghost go to therapy?",
    # #   "punchline": "He had too many boo-issues!"
    # # },
    # # {
    # #   "id": 35,
    # #   "setup": "What soap do ghosts use?",
    # #   "punchline": "BOO-ty wash!"
    # # },
    # # {
    # #   "id": 36,
    # #   "setup": "What's a ghost's favorite TV show?",
    # #   "punchline": "American Horror-Story!"
    # # },
    # # {
    # #   "id": 37,
    # #   "setup": "What do you call a high-ranking ghost?",
    # #   "punchline": "Boo-tenant General!"
    # # },
    # # {
    # #   "id": 38,
    # #   "setup": "What's a ghost's favorite hot beverage?",
    # #   "punchline": "Ghoul-aid!"
    # # },
    # # {
    # #   "id": 39,
    # #   "setup": "What do you get when you cross a ghost with a bee?",
    # #   "punchline": "Boo-bees!"
    # # },
    # # {
    # #   "id": 40,
    # #   "setup": "What's a ghost's favorite medication?",
    # #   "punchline": "Boo-profin!"
    # # },
    # # {
    # #   "id": 41,
    # #   "setup": "How do ghosts sign their emails?",
    # #   "punchline": "Best frights!"
    # # },
    # # {
    # #   "id": 42,
    # #   "setup": "What's a ghost's favorite vegetable?",
    # #   "punchline": "Boo-ccoli!"
    # # },
    # # {
    # #   "id": 43,
    # #   "setup": "How do ghosts send messages?",
    # #   "punchline": "By scare-mail!"
    # # },
    # # {
    # #   "id": 44,
    # #   "setup": "What's a ghost's favorite cookie?",
    # #   "punchline": "Boo-scotti!"
    # # },
    # # {
    # #   "id": 45,
    # #   "setup": "What do you call a ghost's favorite pastime?",
    # #   "punchline": "BOO-ling!"
    # # },
    # # {
    # #   "id": 46,
    # #   "setup": "What does a ghost wear to bed?",
    # #   "punchline": "Boo-jamas!"
    # # },
    # # {
    # #   "id": 47,
    # #   "setup": "What do you call a ghost chef?",
    # #   "punchline": "A booker!"
    # # },
    # # {
    # #   "id": 48,
    # #   "setup": "What's a ghost's favorite movie?",
    # #   "punchline": "The Blair Witch Boo-ject!"
    # # },
    # # {
    # #   "id": 49,
    # #   "setup": "Why are ghosts bad at lying?",
    # #   "punchline": "Because you can see right through them!"
    # # },
    # # {
    # #   "id": 50,
    # #   "setup": "What do you call a ghost comedian?",
    # #   "punchline": "A real scream!"
    # # },
    # {"id": "1", "setup": "Why did the ghost go to the party?", "punchline": "Because he heard it was going to be a boo-last!"},
    # {"id": "2", "setup": "What does a panda ghost eat?", "punchline": "Bamboo!"},
    # {"id": "3", "setup": "What do you call a ghost comedian?", "punchline": "A boo-mer!"},
    # {"id": "4", "setup": "Why did the ghost break up with his girlfriend?", "punchline": "She said he was too boo-ring!"},
    # {"id": "5", "setup": "What’s a ghost’s favorite kind of fruit?", "punchline": "Boo-berries!"},
    # {"id": "6", "setup": "Why did the ghost go into the bar?", "punchline": "For the boos!"},
    # {"id": "7", "setup": "What do you say when you surprise a ghost?", "punchline": "Boo-hoo!"},
    # {"id": "8", "setup": "Why don’t ghosts make good cheerleaders?", "punchline": "Because they always say boo!"},
    # {"id": "9", "setup": "What do you call a ghost detective?", "punchline": "Sherlock Boo-lmes!"},
    # {"id": "10", "setup": "Why did the ghost get a ticket?", "punchline": "He was caught boo-speeding!"},
    # {"id": "11", "setup": "What do ghosts put in their coffee?", "punchline": "Boo-cream!"},
    # {"id": "12", "setup": "What do you call a ghost who loves classical music?", "punchline": "Boo-thoven!"},
    # {"id": "13", "setup": "What’s a ghost’s favorite exercise?", "punchline": "Boo-t camp!"},
    # {"id": "14", "setup": "Why do ghosts hate the rain?", "punchline": "It dampens their spirits!"},
    # {"id": "15", "setup": "What do you call a ghost’s favorite dessert?", "punchline": "Boo-lé!"},
    # {"id": "16", "setup": "What’s a ghost’s favorite ride at the amusement park?", "punchline": "The roller-boo-ster!"},
    # {"id": "17", "setup": "Why did the ghost go to school?", "punchline": "To improve his boo-cabulary!"},
    # {"id": "18", "setup": "What do you call a haunted chicken?", "punchline": "Poultrygeist!"},
    # {"id": "19", "setup": "What’s a ghost’s favorite game?", "punchline": "Hide and shriek!"},
    # {"id": "20", "setup": "Why did the ghost take the elevator?", "punchline": "Because it was a boo-st!"},
    # {"id": "21", "setup": "Why did the ghost get kicked out of the theater?", "punchline": "He kept yelling ‘Boo!’"},
    # {"id": "22", "setup": "What do you call a sad ghost?", "punchline": "A boo-hoo!"},
    # {"id": "23", "setup": "What’s a ghost’s favorite bedtime story?", "punchline": "Boo-ty and the Beast!"},
    # {"id": "24", "setup": "Why do ghosts love elevators?", "punchline": "They lift their spirits!"},
    # {"id": "25", "setup": "What’s a ghost’s favorite instrument?", "punchline": "The boo-kulele!"},
    # {"id": "26", "setup": "What do you call a ghost in a rock band?", "punchline": "Boo Jovi!"},
    # {"id": "27", "setup": "Why don’t ghosts like rain?", "punchline": "Because it gives them chills!"},
    # {"id": "28", "setup": "What’s a ghost’s favorite social media platform?", "punchline": "Boo-ker!"},
    # {"id": "29", "setup": "Why did the ghost fail math?", "punchline": "He couldn’t count on his fingers!"},
    # {"id": "30", "setup": "Why do ghosts hate the wind?", "punchline": "Because it blows them away!"},
    # {"id": "31", "setup": "What’s a ghost’s favorite party activity?", "punchline": "Boo-loon animals!"},
    # {"id": "32", "setup": "What do you call a ghost’s autobiography?", "punchline": "Boo-ography!"},
    # {"id": "33", "setup": "What do you call a ghost’s dream job?", "punchline": "A boo-siness owner!"},
    # {"id": "34", "setup": "What’s a ghost’s favorite winter sport?", "punchline": "Boo-bogganing!"},
    # {"id": "35", "setup": "Why did the ghost become a stand-up comedian?", "punchline": "He was great at boo-sting the crowd!"},
    # {"id": "36", "setup": "What do you call a fashionable ghost?", "punchline": "Boo-tique shopper!"},
    # {"id": "37", "setup": "What’s a ghost’s favorite exercise?", "punchline": "Boo-ty squats!"},
    # {"id": "38", "setup": "Why do ghosts hate spicy food?", "punchline": "Because it gives them the chills!"},
    # {"id": "39", "setup": "What do ghosts do before eating dinner?", "punchline": "They say ‘Boo-n Appétit!’"},
    # {"id": "40", "setup": "What’s a ghost’s favorite romantic movie?", "punchline": "Casperablanca!"},
    # {"id": "41", "setup": "Why don’t ghosts use elevators?", "punchline": "They prefer to take the scare-case!"},
    # {"id": "42", "setup": "What’s a ghost’s favorite cereal?", "punchline": "Boo-berry crunch!"},
    # {"id": "43", "setup": "What do ghosts do at the gym?", "punchline": "Boo-ty building!"},
    # {"id": "44", "setup": "Why did the ghost sit alone at lunch?", "punchline": "He was a little boo-shy!"},
    # {"id": "45", "setup": "What’s a ghost’s favorite type of music?", "punchline": "Boo-gie woogie!"},
    # {"id": "46", "setup": "Why did the ghost refuse to fight?", "punchline": "He didn’t have the guts!"},
    # {"id": "47", "setup": "What do ghosts wear in the rain?", "punchline": "Boo-ts!"},
    # {"id": "48", "setup": "Why don’t ghosts like shopping?", "punchline": "Too many hidden fees!"},
    # {"id": "49", "setup": "What do ghosts order at fast food places?", "punchline": "Boo-gers and fries!"},
    # {"id": "50", "setup": "What’s a ghost’s favorite kind of dance?", "punchline": "The boo-gie! "}

    # claude cat coffee ghost
    {"id": "001", "setup": "Why don't ghost cats drink espresso?", "punchline": "Because they prefer their coffee with a little more boo-st!"},
    {"id": "002", "setup": "What do you call a phantom cat that works at Starbucks?", "punchline": "A boo-rista!"},
    {"id": "003", "setup": "Why did the ghost cat refuse decaf?", "punchline": "Because it needed something with more spirit!"},
    {"id": "004", "setup": "What's a ghost cat's favorite coffee drink?", "punchline": "A frapp\u00e9-phantom!"},
    {"id": "005", "setup": "Why don't ghost cats use coffee filters?", "punchline": "Because they like their coffee supernatural!"},
    {"id": "006", "setup": "What do you call a dead cat that still makes coffee?", "punchline": "A zombie barista!"},
    {"id": "007", "setup": "Why did the phantom cat open a coffee shop?", "punchline": "To serve supernatural lattes!"},
    {"id": "008", "setup": "What's a ghost cat's favorite part of coffee preparation?", "punchline": "The grinding - it reminds them of their haunting days!"},
    {"id": "009", "setup": "Why do ghost cats love cold brew?", "punchline": "Because they're already used to being chilled to the bone!"},
    {"id": "010", "setup": "What do you call a cat spirit that reviews coffee shops?", "punchline": "A phantom food critic!"},
    {"id": "011", "setup": "Why don't cats make good baristas?", "punchline": "Because they always paws at the wrong moment!"},
    {"id": "012", "setup": "What do you call a cat that drinks too much coffee?", "punchline": "Hyper-active!"},
    {"id": "013", "setup": "Why did the cat refuse to drink coffee?", "punchline": "It preferred tea because it's more paw-lite!"},
    {"id": "014", "setup": "What's a cat's favorite coffee temperature?", "punchline": "Purr-fectly warm!"},
    {"id": "015", "setup": "Why do cats hate instant coffee?", "punchline": "Because they prefer things that are fresh-ground!"},
    {"id": "016", "setup": "What do you call a cat working at a coffee plantation?", "punchline": "A bean counter!"},
    {"id": "017", "setup": "Why don't cats drink espresso shots?", "punchline": "Because they're already wired enough!"},
    {"id": "018", "setup": "What's a cat's favorite coffee shop activity?", "punchline": "Laptop warming!"},
    {"id": "019", "setup": "Why did the cat become a coffee critic?", "punchline": "It had refined whiskers for taste!"},
    {"id": "020", "setup": "What do you call a cat that steals coffee?", "punchline": "A cat burglar-ista!"},
    {"id": "021", "setup": "Why don't ghosts drink hot coffee?", "punchline": "Because it goes right through them!"},
    {"id": "022", "setup": "What's a ghost's favorite coffee additive?", "punchline": "Scream and sugar!"},
    {"id": "023", "setup": "Why did the ghost refuse French roast?", "punchline": "It was too dark for their taste!"},
    {"id": "024", "setup": "What do you call a haunted coffee machine?", "punchline": "An espresso-geist!"},
    {"id": "025", "setup": "Why do spirits love coffee shops?", "punchline": "Because of all the medium roasts!"},
    {"id": "026", "setup": "What's a phantom's favorite coffee drink order?", "punchline": "A double shot of boo!"},
    {"id": "027", "setup": "Why don't ghosts work at coffee shops?", "punchline": "Because they can't espresso themselves properly!"},
    {"id": "028", "setup": "What do you call coffee made by ghosts?", "punchline": "Supernatural grounds!"},
    {"id": "029", "setup": "Why did the ghost get fired from the coffee shop?", "punchline": "They kept disappearing during rush hour!"},
    {"id": "030", "setup": "What's a poltergeist's favorite part of making coffee?", "punchline": "The rattling of the beans!"},
    {"id": "031", "setup": "Why did the ghost cat fail barista training?", "punchline": "It couldn't master the latte art - everything came out transparent!"},
    {"id": "032", "setup": "What do you call a cat that only drinks coffee at midnight?", "punchline": "A night-mare-iatto!"},
    {"id": "033", "setup": "Why don't phantom cats use coffee cups?", "punchline": "Because they prefer to drink straight from the spirit!"},
    {"id": "034", "setup": "What's a ghost cat's favorite coffee brewing method?", "punchline": "The French press - it's good for pressing their luck!"},
    {"id": "035", "setup": "Why did the zombie cat open a drive-through coffee shop?", "punchline": "For grave-yard shift workers!"},
    {"id": "036", "setup": "What do you call a cat ghost that reviews coffee beans?", "punchline": "A supernatural sommelier!"},
    {"id": "037", "setup": "Why don't vampire cats drink coffee?", "punchline": "They prefer their drinks with more bite!"},
    {"id": "038", "setup": "What's a ghost cat's favorite coffee shop music?", "punchline": "Boo-gie jazz!"},
    {"id": "039", "setup": "Why did the phantom feline refuse iced coffee?", "punchline": "It was already cold as death!"},
    {"id": "040", "setup": "What do you call a cat spirit that predicts coffee trends?", "punchline": "A coffee fortune teller!"},
    {"id": "041", "setup": "Why don't coffee beans ever get scared?", "punchline": "Because they're always grounded!"},
    {"id": "042", "setup": "What do you call coffee that's possessed?", "punchline": "Demon-ic roast!"},
    {"id": "043", "setup": "Why did the coffee shop hire a medium?", "punchline": "To communicate with their old grounds!"},
    {"id": "044", "setup": "What's a ghost's least favorite coffee?", "punchline": "Light roast - they prefer things darker!"},
    {"id": "045", "setup": "Why don't spirits drink cappuccino?", "punchline": "Too much foam - not enough substance!"},
    {"id": "046", "setup": "What do you call a cat that haunts a coffee shop?", "punchline": "A caffeine phantom!"},
    {"id": "047", "setup": "Why did the ghost cat become a coffee influencer?", "punchline": "It had a supernatural following!"},
    {"id": "048", "setup": "What's a phantom cat's favorite coffee accessory?", "punchline": "A boo-tiful mug!"},
    {"id": "049", "setup": "Why don't ghost cats need coffee stirrers?", "punchline": "They can make things swirl with their minds!"},
    {"id": "050", "setup": "What do you call a cat spirit that only drinks organic coffee?", "punchline": "An eco-ghost!"},
    {"id": "051", "setup": "Why did the haunted cat refuse to work at the coffee shop?", "punchline": "The espresso machine was too loud for its afterlife!"},
    {"id": "052", "setup": "What's a ghost cat's favorite coffee shop game?", "punchline": "Hide and go shriek!"},
    {"id": "053", "setup": "Why don't phantom cats ever spill coffee?", "punchline": "Because they have supernatural coordination!"},
    {"id": "054", "setup": "What do you call a cat ghost that makes terrible coffee?", "punchline": "A boo-tista in training!"},
    {"id": "055", "setup": "Why did the spirit cat get promoted at the coffee shop?", "punchline": "It had otherworldly customer service skills!"},
    {"id": "056", "setup": "What's a ghost's favorite coffee shop complaint?", "punchline": "This coffee has no body to it!"},
    {"id": "057", "setup": "Why don't zombie cats make good coffee?", "punchline": "They always forget the living ingredients!"},
    {"id": "058", "setup": "What do you call a cat that haunts coffee plantations?", "punchline": "A bean ghost!"},
    {"id": "059", "setup": "Why did the phantom cat refuse to clean the coffee machine?", "punchline": "It was afraid of crossing over to the other grind!"},
    {"id": "060", "setup": "What's a ghost cat's favorite coffee shop closing time activity?", "punchline": "Rattling the chairs onto the tables!"},

    # # gpt cat coffee ghost
    # {"id": "61", "setup": "Why did the cat sit on the barista's lap?", "punchline": "It heard there was a purr-essure brew going on."},
    # {"id": "62", "setup": "What do you call a cat who drinks coffee all night?", "punchline": "A caffeind."},
    # {"id": "63", "setup": "Why did the ghost adopt a cat?", "punchline": "Because it needed something else that could randomly knock things off shelves at 3am."},
    # {"id": "64", "setup": "What\u2019s a barista cat\u2019s favorite drink?", "punchline": "A meowcchiato."},
    # {"id": "65", "setup": "Why don\u2019t haunted houses have coffee machines?", "punchline": "The ghosts keep whispering \u201cboo\u201d every time the pot brews."},
    # {"id": "66", "setup": "Why did the cat avoid the espresso machine?", "punchline": "It was afraid of grounds for eviction."},
    # {"id": "67", "setup": "How does a ghost cat order coffee?", "punchline": "\u201cI\u2019ll have a boo-rew, hold the scream.\u201d"},
    # {"id": "68", "setup": "Why do black cats never spill their coffee?", "punchline": "Because they\u2019re paw-sitively precise."},
    # {"id": "69", "setup": "Why did the coffee shop hire a ghost?", "punchline": "For their phantom roast blend."},
    # {"id": "70", "setup": "What\u2019s a ghost cat\u2019s favorite kind of roast?", "punchline": "Dark and mysterious."},
    # {"id": "71", "setup": "Why was the espresso haunted?", "punchline": "Because the barista forgot to exorcise the beans."},
    # {"id": "72", "setup": "What do you call it when a cat knocks over your latte?", "punchline": "A cat-astrophe."},
    # {"id": "73", "setup": "Why did the ghost avoid Starbucks?", "punchline": "Too many spirits already inside."},
    # {"id": "74", "setup": "Why did the cat open a coffee shop?", "punchline": "To make meowny while everyone else slept."},
    # {"id": "75", "setup": "What happens when a poltergeist drinks espresso?", "punchline": "They become a shaken not stirred spirit."},
    # {"id": "76", "setup": "How do cats like their coffee?", "punchline": "With a splash of cream... and chaos."},
    # {"id": "77", "setup": "What\u2019s a ghost\u2019s favorite caf\u00e9 drink?", "punchline": "A moan-chiato."},
    # {"id": "78", "setup": "Why was the cat always tired despite drinking coffee?", "punchline": "It only used it to chase phantom mice."},
    # {"id": "79", "setup": "What did the barista say to the sneaky ghost?", "punchline": "\u201cYou\u2019ve bean lurking here too long.\u201d"},
    # {"id": "80", "setup": "Why did the cat refuse the decaf?", "punchline": "Because it said, \u201cI need my claws fully charged!\u201d"},
    # {"id": "81", "setup": "What did the haunted coffee say to the cup?", "punchline": "\u201cYou can\u2019t espresso how cursed I am.\u201d"},
    # {"id": "82", "setup": "Why do ghost cats avoid lattes?", "punchline": "They vanish in the foam."},
    # {"id": "83", "setup": "How does a ghost pay for coffee?", "punchline": "With crypt-o-currency."},
    # {"id": "84", "setup": "Why did the ghost open a caf\u00e9?", "punchline": "Because business was dead everywhere else."},
    # {"id": "85", "setup": "What do you call a tired cat who sees spirits?", "punchline": "Para-normal."},
    # {"id": "86", "setup": "Why did the espresso scream?", "punchline": "It saw a ghost bean."},
    # {"id": "87", "setup": "What do ghosts and cold brew have in common?", "punchline": "They both creep up on you."},
    # {"id": "88", "setup": "Why did the cat summon a ghost?", "punchline": "To blame someone else for the broken mug."},
    # {"id": "89", "setup": "What\u2019s a ghost cat\u2019s job at a coffee shop?", "punchline": "Spilling beans\u2014literally and figuratively."},
    # {"id": "90", "setup": "Why don\u2019t cats believe in ghosts?", "punchline": "Because they are the ghosts in the night."},
    # {"id": "91", "setup": "What do you get when a cat haunts a coffee shop?", "punchline": "A meow-ling presence."},
    # {"id": "92", "setup": "Why did the ghost avoid pumpkin spice lattes?", "punchline": "Too basic for the afterlife."},
    # {"id": "93", "setup": "Why did the cat get a Ouija board?", "punchline": "It wanted to purr-suade the spirits to refill its bowl."},
    # {"id": "94", "setup": "How do ghost cats flirt at the caf\u00e9?", "punchline": "\u201cAre you an espresso? Because you've possessed me.\u201d"},
    # {"id": "95", "setup": "Why did the barista faint?", "punchline": "A ghost tried to tip them in ectoplasm."},
    # {"id": "96", "setup": "Why was the haunted espresso machine screaming?", "punchline": "Someone used cursed beans."},
    # {"id": "97", "setup": "What\u2019s a cat\u2019s favorite coffee spell?", "punchline": "Abra-cappuccino!"},
    # {"id": "98", "setup": "What kind of milk do ghost cats prefer?", "punchline": "Ecto-lactose."},
    # {"id": "99", "setup": "Why did the spirit barista lose their job?", "punchline": "They kept vanishing during rush hour."},
    # {"id": "100", "setup": "Why don\u2019t ghost cats sleep on your chest?", "punchline": "They haunt your dreams instead."},
    # {"id": "101", "setup": "How do you know your coffee shop is haunted?", "punchline": "The milk steams itself."},
    # {"id": "102", "setup": "What do cats and cold brew have in common?", "punchline": "Strong personalities and cold, judging stares."},
    # {"id": "103", "setup": "What did the ghost cat say when caught stealing coffee?", "punchline": "\u201cYou never saw me.\u201d"},
    # {"id": "104", "setup": "Why did the ghost spill the coffee?", "punchline": "It couldn't hold the cup... no hands."},
    # {"id": "105", "setup": "What\u2019s a ghost\u2019s favorite coffee blend?", "punchline": "Boo-rista Reserve."},
    # {"id": "106", "setup": "Why don\u2019t cats need alarm clocks?", "punchline": "Their coffee is served by phantoms at 4am."},
    # {"id": "107", "setup": "Why was the black cat banned from the coffee shop?", "punchline": "Too many unexplained incidents."},
    # {"id": "108", "setup": "Why do ghost cats avoid espresso shots?", "punchline": "They already float above the ground."},
    # {"id": "109", "setup": "What do you get when you cross a latte with a haunted house?", "punchline": "A creamy scream."},
    # {"id": "110", "setup": "Why did the cat write a horror story in the caf\u00e9?", "punchline": "It was inspired by the ghost roast."},
]


def tell_a_boo_joke():
    joke = random.choice(jokes)
    print(f"{joke['setup']}")
    time.sleep(2.5)
    print(f"{joke['punchline']}")







