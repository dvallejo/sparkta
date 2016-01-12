/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.sparkta.serving.core.services

import java.util.UUID

import com.stratio.sparkta.serving.core.constants.AppConstant
import com.stratio.sparkta.serving.core.models.{FragmentElementModel, SparktaSerializer}
import com.stratio.common.utils.repository.RepositoryComponent
import com.stratio.common.utils.config.ConfigComponent
import com.stratio.common.utils.logger.LoggerComponent

import org.json4s.jackson.Serialization.{ read, write }

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait FragmentService extends SparktaSerializer {
  self: RepositoryComponent[String, Array[Byte]] with ConfigComponent with LoggerComponent =>

  def findByType(fragmentType: String): Future[Seq[FragmentElementModel]] = Future {
    logger.info(s"> Retrieving information for path: ${fragmentPath(fragmentType)}")
    getByType(fragmentType)
  }

  def findByTypeAndId(fragmentType: String, id: String): Future[FragmentElementModel] = Future {
    logger.info(s"> Retrieving information for path: ${fragmentPath(fragmentType,id)}")
    getByTypeAndId(fragmentType, id)
      .getOrElse(throw NotFoundFragmentException(fragmentType, id))
  }

  def findByTypeAndName(fragmentType: String, name: String): Future[FragmentElementModel] = Future {
    logger.info(s"> Retrieving information for path: ${fragmentPath(fragmentType,id)}")
    getByTypeAndName(fragmentType, name)
      .getOrElse( throw NotFoundFragmentException(fragmentType, name))
  }

  def create(fragment: FragmentElementModel): Future[FragmentElementModel] = Future {
    if (getByTypeAndName(fragment.fragmentType, fragment.name).isDefined)
      throw FragmentConflictException(fragment.fragmentType, fragment.id.getOrElse(fragment.name))
    else {
      val currentId: Option[String] = Option(fragment.id.getOrElse(UUID.randomUUID.toString))
      val newFragment = fragment.copy(id = currentId, name = fragment.name.toLowerCase)

      val fragmentCreated: Array[Byte] =
        repository.create(s"${fragmentPath(newFragment.fragmentType,newFragment.id.get)}", write(newFragment).getBytes)

      read[FragmentElementModel](new String(fragmentCreated))
    }
  }

  def update(fragment: FragmentElementModel): Future[Unit] = Future {
    if (getByTypeAndName(fragment.fragmentType, fragment.name.toLowerCase).isEmpty)
      throw NotFoundFragmentException(fragment.fragmentType, fragment.id.getOrElse(""))
    else
      repository.update(fragmentPath(fragment.fragmentType, fragment.id.get), write(fragment).getBytes)
  }

  def deleteByTypeAndId(fragmentType: String, id: String): Future[Unit] = Future {
    if (getByTypeAndName(fragmentType, id).isEmpty)
      throw NotFoundFragmentException(fragmentType, id)
    else
      repository.delete(s"${fragmentPath(fragmentType)}/$id")
  }

  private def getByType(fragmentType: String): List[FragmentElementModel] =
    repository.getChildren(fragmentPath(fragmentType))
      .flatMap(element =>
        readFragmentElement(fragmentType, element)
      )

  private def getByTypeAndId(fragmentType: String, id: String): Option[FragmentElementModel] =
    getByType(fragmentType).find(_.id == id)

  private def getByTypeAndName(fragmentType: String, name: String): Option[FragmentElementModel] =
    getByType(fragmentType).find(_.name == name)

  private def fragmentPath(fragmentType: String): String =
    fragmentType match {
      case "input" => s"${AppConstant.BaseZKPath}/fragments/input"
      case "output" => s"${AppConstant.BaseZKPath}/fragments/output"
      case _ => throw new IllegalArgumentException("The fragment type must be input|output")
    }

  private def fragmentPath(fragmentType: String, id: String): String =
    fragmentPath(fragmentType) + "/" + id

  private def readFragmentElement(fragmentType: String, element: String): Option[FragmentElementModel] =
    repository.get(s"${fragmentPath(fragmentType, element)}")
      .map(elem =>
        read[FragmentElementModel](new String(elem))
      )
}
