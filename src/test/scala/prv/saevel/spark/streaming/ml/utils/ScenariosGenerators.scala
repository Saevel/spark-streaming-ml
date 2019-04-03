package prv.saevel.spark.streaming.ml.utils

import org.scalacheck.Gen
import prv.saevel.spark.streaming.ml.model.{Client, EducationLevel, FinancialProducts, Sex}

trait ScenariosGenerators {

  private val sexes = Gen.oneOf(Seq(Sex.Male, Sex.Female))

  private val educationLevels =
    Gen.oneOf(Seq(EducationLevel.None, EducationLevel.Primary, EducationLevel.Secondary, EducationLevel.Higher))

  private val professions = Gen.oneOf("Physical Worker", "Teacher", "Plumber", "Driver", "Lawyer",
    "Programmer", "Carpenter", "Unemployed")

  protected val underagePeople: Gen[Client] = for {
    age <- Gen.choose(10, 20)
    sex <- sexes
    educationLevel <- Gen.oneOf(EducationLevel.None, EducationLevel.Primary, EducationLevel.Secondary)
  } yield Client(age, educationLevel, sex, "N/A", FinancialProducts.None)

  protected val poorlyEducated: Gen[Client] = for {
    educationLevel <- Gen.oneOf(EducationLevel.None, EducationLevel.Primary)
    sex <- sexes
    age <- Gen.choose(21, 100)
    profession <- professions
    preference <- Gen.frequency(
      (90, FinancialProducts.ShortTermLoan),
      (2, FinancialProducts.Loan),
      (2, FinancialProducts.Mortgage),
      (2, FinancialProducts.ReverseMortgage),
      (2, FinancialProducts.Securities),
      (2, FinancialProducts.Stocks)
    )
  } yield Client(age, educationLevel, sex, profession, preference)

  private val secondaryOrHigherEducation: Gen[String] = Gen.oneOf(EducationLevel.Secondary, EducationLevel.Higher)

  protected val womenInTheirTwenties: Gen[Client] = for {
    educationLevel <- secondaryOrHigherEducation
    age <- Gen.choose(21, 30)
    profession <- professions
    preference <- Gen.frequency(
      (2, FinancialProducts.ShortTermLoan),
      (90, FinancialProducts.Loan),
      (2, FinancialProducts.Mortgage),
      (2, FinancialProducts.ReverseMortgage),
      (2, FinancialProducts.Securities),
      (2, FinancialProducts.Stocks)
    )
  } yield Client(age, educationLevel, Sex.Female, profession, preference)

  protected val menInTheirTwenties: Gen[Client] = for {
    educationLevel <- secondaryOrHigherEducation
    age <- Gen.choose(21, 30)
    profession <- professions
    preference <- Gen.frequency(
      (5, FinancialProducts.ShortTermLoan),
      (50, FinancialProducts.Loan),
      (25, FinancialProducts.Mortgage),
      (5, FinancialProducts.ReverseMortgage),
      (5, FinancialProducts.Securities),
      (5, FinancialProducts.Stocks)
    )
  } yield Client(age, educationLevel, Sex.Male, profession, preference)

  protected val peopleInTheirThirties: Gen[Client] = for {
    educationLevel <- secondaryOrHigherEducation
    age <- Gen.choose(31, 40)
    sex <- sexes
    profession <- professions
    preference <- Gen.frequency(
      (2, FinancialProducts.ShortTermLoan),
      (2, FinancialProducts.Loan),
      (90, FinancialProducts.Mortgage),
      (2, FinancialProducts.ReverseMortgage),
      (2, FinancialProducts.Securities),
      (2, FinancialProducts.Stocks)
    )
  } yield Client(age, educationLevel, sex, profession, preference)

  protected val womenInTheirFourties: Gen[Client] = for {
    educationLevel <- secondaryOrHigherEducation
    age <- Gen.choose(41, 50)
    profession <- professions
    preference <- Gen.frequency(
      (2, FinancialProducts.ShortTermLoan),
      (2, FinancialProducts.Loan),
      (90, FinancialProducts.Mortgage),
      (2, FinancialProducts.ReverseMortgage),
      (2, FinancialProducts.Securities),
      (2, FinancialProducts.Stocks)
    )
  } yield Client(age, educationLevel, Sex.Female, profession, preference)

  protected val menInTheirFourties: Gen[Client] = for {
    educationLevel <- secondaryOrHigherEducation
    age <- Gen.choose(41, 50)
    profession <- professions
    preference <- Gen.frequency(
      (2, FinancialProducts.ShortTermLoan),
      (20, FinancialProducts.Loan),
      (2, FinancialProducts.Mortgage),
      (2, FinancialProducts.ReverseMortgage),
      (35, FinancialProducts.Securities),
      (35, FinancialProducts.Stocks)
    )
  } yield Client(age, educationLevel, Sex.Female, profession, preference)

  protected val elderlyPeople: Gen[Client] = for {
    educationLevel <- secondaryOrHigherEducation
    age <- Gen.choose(51, 100)
    profession <- professions
    preference <- Gen.frequency(
      (2, FinancialProducts.ShortTermLoan),
      (90, FinancialProducts.Loan),
      (2, FinancialProducts.Mortgage),
      (2, FinancialProducts.ReverseMortgage),
      (2, FinancialProducts.Securities),
      (2, FinancialProducts.Stocks)
    )
  } yield Client(age, educationLevel, Sex.Female, profession, preference)

  protected def allTypesOfClients(n: Int): Gen[List[Client]] = Gen.listOfN(n, Gen.oneOf(
    poorlyEducated,
    womenInTheirTwenties,
    menInTheirTwenties,
    peopleInTheirThirties,
    womenInTheirFourties,
    menInTheirFourties,
    elderlyPeople
  ))
}
