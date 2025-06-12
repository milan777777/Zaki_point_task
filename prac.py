def teacher_message(school_name):
    print(f"Welcome to {school_name} School")

def calculate_boy_girl_ratio(boys, girls):
    if int(girls) == 0:
        return "No girls to compare"
    return round(int(boys) / int(girls), 2)

class School:
    def __init__(self, teachers, students, school_name, area=25):
        self.teachers = teachers
        self.students = students
        self.school_name = school_name 
        self.area = area
        self.sports = []
        self.grade = {'course_name': 'grade'}
        self.absent = False
        self.house = set()

    def add_grade(self, course_name, grade):
        self.grade[course_name] = grade
        print(self.grade)

    def add_sports(self, sport, type):
        self.sports.append((sport, type))
        print(self.sports)

    def mark_absent(self, status):
        self.absent = status
        print("Result:", self.absent)

    def add_house(self, house_name):
        self.house.add(house_name)
        print("house:", self.house)

    def show_boy_girl_ratio(self, boys, girls):  
        ratio = calculate_boy_girl_ratio(boys, girls)
        print("Boy-Girl Ratio:", ratio)

sch = School("10", "100", "Triiii")
teacher_message(sch.school_name)
sch.add_grade("Maths", "A")
sch.add_sports("football", "forward")
sch.mark_absent(False)
sch.add_house("Green")
sch.show_boy_girl_ratio(60, 40) 
