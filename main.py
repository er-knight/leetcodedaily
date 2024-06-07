from calendar import monthrange
from datetime import datetime, timedelta
from pathlib import Path
from random import shuffle
from time import sleep
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine, String, Integer, Float, DateTime, select, insert, update, and_, or_
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import DeclarativeBase, mapped_column, Session


BASE_URL = "https://leetcode.com"
PROBLEMSET_URL = "https://leetcode.com/problemset/?page={page}"
PROBLEMS_DB_PATH = Path(__file__).parent / "problems.db"
PROBLEMS_DB_URI = f"sqlite:///{PROBLEMS_DB_PATH}"


class Base(DeclarativeBase):
    pass


class Problem(Base):
    __tablename__ = "problems"

    id = mapped_column(Integer, primary_key=True)
    title = mapped_column(String(256))
    url = mapped_column(String(256))
    acceptance_rate = mapped_column(Float)
    difficulty = mapped_column(String(8))
    last_included = mapped_column(DateTime)

    def __repr__(self) -> str:
        return f"{self.id}. {self.title}"


class ProblemDate(Base):
    __tablename__ = "problem_dates"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    problem_id = mapped_column(Integer)
    included_at = mapped_column(DateTime)


def main():

    engine = create_engine(PROBLEMS_DB_URI, connect_args={
                           "check_same_thread": False}, echo=True)
    Base.metadata.create_all(engine)

    page = 1

    driver = webdriver.Chrome()
    driver.get(PROBLEMSET_URL.format(page=page))

    sleep(3)

    with Session(engine) as session:
        while True:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            problems_html = soup.find_all(
                "div", attrs={"role": "rowgroup"})[2].contents

            problems = []
            for problem in problems_html:
                if not problem.contents[0].contents:
                    # means not a daily coding problem and not a premium problem
                    anchor_tag = problem.contents[1].find("a")
                    if anchor_tag:
                        _id, title = anchor_tag.string.split(maxsplit=1)
                        _id = int(_id.removesuffix("."))
                        url = BASE_URL + anchor_tag["href"]

                    acceptance_rate = float(
                        problem.contents[3].contents[0].string.removesuffix("%"))
                    difficulty = problem.contents[4].contents[0].string

                    print(_id, title, url, acceptance_rate, difficulty)
                    problems.append({
                        "id": _id, "title": title, "url": url,
                        "acceptance_rate": acceptance_rate,
                        "difficulty": difficulty
                    })

            insert_stmt = sqlite_insert(Problem).values(problems)

            session.execute(
                insert_stmt
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_=dict(
                        acceptance_rate=insert_stmt.excluded.acceptance_rate,
                        difficulty=insert_stmt.excluded.difficulty
                    )
                )
            )

            session.commit()

            navigation = driver.find_element(
                By.CSS_SELECTOR, "[role=navigation]")
            next_button = navigation.find_element(
                By.CSS_SELECTOR, "[aria-label=next]")

            if not next_button.is_enabled():
                break

            next_button.click()
            page += 1

            sleep(3)

        before_90_days = datetime.now(tz=ZoneInfo("UTC")) - timedelta(days=90)
        all_problems = session.execute(
            select(Problem.id, Problem.difficulty)
            .where(
                or_(
                    Problem.last_included == None,
                    Problem.last_included < before_90_days
                ),
                or_(
                    and_(Problem.difficulty == 'Easy',
                         Problem.acceptance_rate < 30),
                    and_(Problem.difficulty == 'Medium',
                         Problem.acceptance_rate < 60),
                    Problem.difficulty == 'Hard'
                )
            )
        ).fetchall()

        month_start = datetime.now(tz=ZoneInfo("UTC")).replace(month=8,
            day=1, hour=0, minute=0, second=0, microsecond=0)
        num_days = monthrange(month_start.year, month_start.month)[1]

        frequency = {}
        for _, difficulty in all_problems:
            frequency[difficulty] = frequency.get(difficulty, 0) + 1

        total_problems = len(all_problems)

        easy_count = num_days * frequency["Easy"] / total_problems
        medium_count = num_days * frequency["Medium"] / total_problems
        hard_count = num_days * frequency["Hard"] / total_problems

        delta = num_days - (easy_count + medium_count + hard_count)
        medium_count += delta

        shuffle(all_problems)

        problems, day = [], 0
        for _id, difficulty in all_problems:
            if day == num_days:
                break
            
            if difficulty == "Easy" and easy_count > 0:
                easy_count -= 1
                problems.append(
                    {"id": _id, "last_included": month_start + timedelta(days=day)})
                day += 1
            elif difficulty == "Medium" and medium_count > 0:
                medium_count -= 1
                problems.append(
                    {"id": _id, "last_included": month_start + timedelta(days=day)})
                day += 1
            elif difficulty == "Hard" and hard_count > 0:
                hard_count -= 1
                problems.append(
                    {"id": _id, "last_included": month_start + timedelta(days=day)})
                day += 1

        session.execute(update(Problem), problems)
        session.execute(insert(ProblemDate), [
                        {"problem_id": problem["id"], "included_at": problem["last_included"]} for problem in problems])
        session.commit()


if __name__ == "__main__":

    main()
